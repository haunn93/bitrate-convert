import { GetObjectCommand, HeadObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { spawn } from 'child_process';
import fs, { appendFileSync, createWriteStream, existsSync, writeFileSync } from 'fs';
import { access, mkdir, readFile, unlink } from 'fs/promises';
import { google } from 'googleapis';
import * as path from 'path';
import readline from 'readline';
import dotenv from 'dotenv';
dotenv.config();

//command line arguments
const args = process.argv.slice(2);
const currentInstance = parseInt(args[0] || '0', 10); // Default to 0 if not specified
const totalInstances = parseInt(args[1] || '1', 10); // Default to 1 if not specified
const useCPU = args[2] === 'cpu'; // Use 'cpu' as third parameter to use CPU encoding

console.log(`🔢 Running as instance ${currentInstance} of ${totalInstances}`);
console.log(`🖥️ Using ${useCPU ? 'CPU (libx264)' : 'GPU (h264_nvenc)'} for encoding`);

const BUCKET = process.env.BUCKET;
console.log('🚀 ~ BUCKET:', BUCKET);
const REGION = process.env.REGION;
console.log('🚀 ~ REGION:', REGION);
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;

// Add this option to your command line arguments at the top of the file
const scanForDuplicates = args[3] === 'scan-duplicates';

// Add this to your transcodeFile function, after drive is initialized

async function scanGoogleDriveForDuplicates(drive, folderId) {
  console.log('🔍 Scanning Google Drive for duplicate filenames...');

  try {
    // Get all files in the folder and subfolders
    const files = await getAllFilesInFolder(drive, folderId);

    // Create a map to track filenames and their occurrences
    const fileMap = {};
    const duplicates = [];

    // Process each file
    for (const file of files) {
      if (!fileMap[file.name]) {
        fileMap[file.name] = [
          {
            id: file.id,
            name: file.name,
            parentId: file.parents ? file.parents[0] : folderId,
          },
        ];
      } else {
        // This is a duplicate
        fileMap[file.name].push({
          id: file.id,
          name: file.name,
          parentId: file.parents ? file.parents[0] : folderId,
        });

        // Only add to duplicates array if this is the first duplicate found
        if (fileMap[file.name].length === 2) {
          duplicates.push(file.name);
        }
      }
    }

    // Log duplicate files
    if (duplicates.length > 0) {
      console.log(`🔄 Found ${duplicates.length} files with duplicate names in Google Drive`);

      // List each duplicate with its locations
      for (const name of duplicates) {
        console.log(`\n📄 Duplicate: "${name}"`);
        for (const instance of fileMap[name]) {
          const parent = await getParentFolderName(drive, instance.parentId);
          console.log(`   - ID: ${instance.id} (Folder: ${parent})`);
        }
      }

      return fileMap;
    } else {
      console.log('✅ No duplicate filenames found in Google Drive');
      return null;
    }
  } catch (error) {
    console.error('❌ Error scanning for duplicates:', error.message);
    return null;
  }
}

// Helper function to get all files in a folder and its subfolders
async function getAllFilesInFolder(drive, folderId) {
  let allFiles = [];
  let pageToken = null;

  try {
    do {
      // Get files in current folder
      const response = await drive.files.list({
        q: `'${folderId}' in parents and trashed=false`,
        fields: 'nextPageToken, files(id, name, mimeType, parents)',
        pageToken: pageToken,
        pageSize: 1000,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      // Process response
      const files = response.data.files || [];
      pageToken = response.data.nextPageToken;

      // Add files to our list and recursively process subfolders
      for (const file of files) {
        if (file.mimeType === 'application/vnd.google-apps.folder') {
          // Recursively get files from subfolder
          const subfolderFiles = await getAllFilesInFolder(drive, file.id);
          allFiles = allFiles.concat(subfolderFiles);
        } else {
          // Add file to our list
          allFiles.push(file);
        }
      }
    } while (pageToken);

    return allFiles;
  } catch (error) {
    console.error(`Error getting files in folder ${folderId}:`, error.message);
    return allFiles; // Return what we have so far
  }
}

async function trashDuplicatesExceptFirst(drive, duplicateMap) {
  let trashedCount = 0;
  let permissionErrors = 0;
  let notFoundErrors = 0;

  // Get all file IDs to trash for batch processing
  const fileIdsToTrash = [];
  const fileInfoMap = {}; // Map file IDs to names for reporting

  for (const fileName in duplicateMap) {
    const instances = duplicateMap[fileName];
    if (instances.length > 1) {
      // Skip the first one, collect all others
      for (let i = 1; i < instances.length; i++) {
        fileIdsToTrash.push(instances[i].id);
        fileInfoMap[instances[i].id] = {
          name: fileName,
          parentId: instances[i].parentId,
        };
      }
    }
  }

  if (fileIdsToTrash.length === 0) {
    console.log('🤷‍♂️ No duplicate files to trash');
    return { trashedCount, permissionErrors, notFoundErrors };
  }

  console.log(`\n🔍 Found ${fileIdsToTrash.length} duplicate files to process`);

  // Ask for confirmation
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  console.log(
    '\n⚠️ WARNING: This will move all duplicate instances except the first one to trash.'
  );
  console.log(`   ${fileIdsToTrash.length} files will be moved to trash.`);
  console.log('   Do you want to continue? (y/n)');

  const confirm = await new Promise((resolve) => {
    rl.question('> ', resolve);
  });

  rl.close();

  if (confirm.toLowerCase() !== 'y') {
    console.log('❌ Trash operation cancelled by user');
    return { trashedCount, permissionErrors, notFoundErrors };
  }

  // Use batch trash for efficiency
  console.log(`🗑️ Moving ${fileIdsToTrash.length} files to trash in batches...`);
  const results = await batchTrashFiles(drive, fileIdsToTrash);

  // Report results
  console.log(`\n📊 Batch Trash Results:`);
  console.log(`✅ Successfully trashed: ${results.success} files`);
  console.log(`⚠️ Files not found: ${results.notFound}`);
  console.log(`⛔ Permission denied: ${results.permissionDenied}`);

  console.log(`\n🔔 Note: Trashed files can be recovered from Google Drive trash for 30 days`);

  return results;
}

async function handleDuplicates(drive, duplicateMap) {
  if (!duplicateMap) return;

  let totalDuplicates = 0;
  for (const fileName in duplicateMap) {
    totalDuplicates += duplicateMap[fileName].length - 1;
  }

  console.log(
    `\n📊 Found ${
      Object.keys(duplicateMap).length
    } files with duplicates (${totalDuplicates} total duplicates)`
  );

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  // First ask if they want to verify files exist
  console.log('\n🔍 Would you like to verify all files exist before proceeding? (y/n)');
  console.log('   This can take a while for large numbers of files.');

  const verifyAnswer = await new Promise((resolve) => {
    rl.question('> ', resolve);
  });

  // If they want to verify, run the refresh
  if (verifyAnswer.toLowerCase() === 'y') {
    console.log('🔄 Refreshing duplicate file map...');
    duplicateMap = await refreshDuplicateMap(drive, duplicateMap);
  } else {
    console.log('⏩ Skipping file verification.');
  }

  // Now ask about handling duplicates
  console.log('\n🗑️ Would you like to handle duplicate files? (y/n)');

  const answer = await new Promise((resolve) => {
    rl.question('> ', resolve);
  });

  if (answer.toLowerCase() === 'y') {
    console.log('\nOptions for handling duplicates:');
    console.log('1️⃣ Keep newest version of each file (delete older duplicates)');
    console.log('2️⃣ Keep first instance of each file (delete all duplicates)');
    console.log('3️⃣ Rename duplicates to include parent folder name');
    console.log('4️⃣ Move duplicates to trash instead of deleting');
    console.log('5️⃣ List duplicates but take no action');

    const action = await new Promise((resolve) => {
      rl.question('Select option (1-5): ', resolve);
    });

    switch (action) {
      case '1':
        await deleteOlderDuplicates(drive, duplicateMap);
        break;
      case '2':
        console.log('Skip verification and directly delete duplicates? (y/n)');
        const skipVerify = await new Promise((resolve) => {
          rl.question('> ', resolve);
        });
        await deleteAllDuplicatesExceptFirst(drive, duplicateMap, skipVerify.toLowerCase() === 'y');
        break;
      case '3':
        await renameDuplicates(drive, duplicateMap);
        break;
      case '4':
        await trashDuplicatesExceptFirst(drive, duplicateMap);
        break;
      case '5':
      default:
        console.log('📋 No action taken. List of duplicates generated.');
        break;
    }
  }

  rl.close();
}

// Helper function to delete older duplicates
async function deleteOlderDuplicates(drive, duplicateMap) {
  // This requires file creation date, which we need to fetch
  // Implementation depends on comparing file metadata
}

// Helper function to delete all duplicates except first instance
async function deleteAllDuplicatesExceptFirst(drive, duplicateMap, skipVerification = false) {
  let deletedCount = 0;
  let permissionErrors = 0;
  let notFoundErrors = 0;

  // Get all file IDs to delete for batch processing
  const fileIdsToDelete = [];
  const fileInfoMap = {}; // Map file IDs to names for reporting

  for (const fileName in duplicateMap) {
    const instances = duplicateMap[fileName];
    if (instances.length > 1) {
      // Skip the first one, collect all others
      for (let i = 1; i < instances.length; i++) {
        fileIdsToDelete.push(instances[i].id);
        fileInfoMap[instances[i].id] = {
          name: fileName,
          parentId: instances[i].parentId,
        };
      }
    }
  }

  if (fileIdsToDelete.length === 0) {
    console.log('🤷‍♂️ No duplicate files to delete');
    return { deletedCount, permissionErrors, notFoundErrors };
  }

  console.log(`\n🔍 Found ${fileIdsToDelete.length} duplicate files to process`);

  // If not skipping verification, ask for confirmation
  if (!skipVerification) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    console.log('\n⚠️ WARNING: This will delete all duplicate instances except the first one.');
    console.log(`   ${fileIdsToDelete.length} files will be permanently deleted.`);
    console.log('   Do you want to continue? (y/n)');

    const confirm = await new Promise((resolve) => {
      rl.question('> ', resolve);
    });

    rl.close();

    if (confirm.toLowerCase() !== 'y') {
      console.log('❌ Deletion cancelled by user');
      return { deletedCount, permissionErrors, notFoundErrors };
    }
  }

  // Use batch delete for efficiency
  console.log(`🗑️ Deleting ${fileIdsToDelete.length} files in batches...`);
  const results = await batchDeleteFiles(drive, fileIdsToDelete);

  // Report results
  console.log(`\n📊 Batch Delete Results:`);
  console.log(`✅ Successfully deleted: ${results.success} files`);
  console.log(`⚠️ Files not found: ${results.notFound}`);
  console.log(`⛔ Permission denied: ${results.permissionDenied}`);

  return results;
}

async function refreshDuplicateMap(drive, duplicateMap) {
  console.log(
    `🔍 Verifying existence of ${Object.keys(duplicateMap).length} file types with duplicates...`
  );
  const refreshedMap = {};
  let removedCount = 0;
  let processedFiles = 0;
  let totalFiles = 0;

  // Count total files to process
  for (const fileName in duplicateMap) {
    totalFiles += duplicateMap[fileName].length;
  }

  // Process files with a timeout for the entire operation
  const startTime = Date.now();
  const timeout = 5 * 60 * 1000; // 5 minutes timeout

  try {
    for (const fileName in duplicateMap) {
      // Check if overall operation is taking too long
      if (Date.now() - startTime > timeout) {
        console.warn(
          `⚠️ Refresh operation taking too long (${Math.round(
            (Date.now() - startTime) / 1000
          )}s), returning partial results`
        );
        break;
      }

      refreshedMap[fileName] = [];

      // Process each instance for this filename
      for (const instance of duplicateMap[fileName]) {
        processedFiles++;

        // Show progress every 10 files
        if (processedFiles % 10 === 0 || processedFiles === totalFiles) {
          const percent = Math.round((processedFiles / totalFiles) * 100);
          console.log(`🔄 Refreshing file map: ${percent}% (${processedFiles}/${totalFiles})`);
        }

        try {
          // Use a promise with timeout to prevent hanging on a single request
          const fileExists = await Promise.race([
            drive.files
              .get({
                fileId: instance.id,
                fields: 'id',
                supportsAllDrives: true,
              })
              .then(() => true)
              .catch((e) => {
                if (e.message.includes('File not found')) return false;
                throw e;
              }),
            new Promise((resolve) =>
              setTimeout(() => {
                console.warn(`⚠️ Timeout checking file ${instance.id}, assuming it exists`);
                resolve(true);
              }, 10000)
            ), // 10 second timeout per file check
          ]);

          if (fileExists) {
            refreshedMap[fileName].push(instance);
          } else {
            removedCount++;
          }
        } catch (error) {
          console.error(`⚠️ Error checking file ${fileName} (${instance.id}):`, error.message);
          // Include file in refreshed map even if we can't verify it
          refreshedMap[fileName].push(instance);
        }

        // Add a small delay between API calls to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      // If no instances remain, remove the entry entirely
      if (refreshedMap[fileName].length === 0) {
        delete refreshedMap[fileName];
      }
    }

    const totalDuplicatesRemaining = Object.values(refreshedMap).reduce(
      (acc, instances) => acc + instances.length,
      0
    );

    console.log(`✅ Refresh complete: ${removedCount} stale files removed`);
    console.log(
      `📊 Remaining duplicates: ${totalDuplicatesRemaining} files across ${
        Object.keys(refreshedMap).length
      } filenames`
    );

    return refreshedMap;
  } catch (error) {
    console.error('❌ Error refreshing duplicate map:', error);
    console.log('⚠️ Returning original duplicate map due to refresh error');
    return duplicateMap; // Return original map on error
  }
}

// Helper function to rename duplicates
async function renameDuplicates(drive, duplicateMap) {
  let renamedCount = 0;

  for (const fileName in duplicateMap) {
    const instances = duplicateMap[fileName];
    if (instances.length > 1) {
      // Skip the first instance, rename the others
      for (let i = 1; i < instances.length; i++) {
        try {
          const parentFolder = await getParentFolderName(drive, instances[i].parentId);
          const newName = `${fileName} (${parentFolder})`;

          await drive.files.update({
            fileId: instances[i].id,
            resource: { name: newName },
            supportsAllDrives: true,
          });

          console.log(`✏️ Renamed duplicate: ${fileName} → ${newName}`);
          renamedCount++;
        } catch (error) {
          console.error(`❌ Error renaming file ${fileName}:`, error.message);
        }
      }
    }
  }

  console.log(`✅ Renamed ${renamedCount} duplicate files`);
}

// Helper function to get folder name from ID
async function getParentFolderName(drive, folderId) {
  try {
    const response = await drive.files.get({
      fileId: folderId,
      fields: 'name',
      supportsAllDrives: true,
    });
    return response.data.name;
  } catch (error) {
    return `Unknown Folder (${folderId})`;
  }
}

function extractCameraId(filePath) {
  const matches = filePath.match(/camera-(\d+)/);
  if (matches && matches.length > 1) {
    return `camera-${matches[1]}`;
  }
  return 'other-videos'; // Default folder name if no camera ID found
}

async function initErrorLog() {
  const errorLogFile = 'error_transcode.txt';

  if (!existsSync(errorLogFile)) {
    try {
      // Create the file with header
      const timestamp = new Date().toISOString();
      const header = `Transcoding Error Log - Created at ${timestamp}\n`;
      writeFileSync(errorLogFile, header);
      console.log(`📝 Created error log file: ${errorLogFile}`);
    } catch (err) {
      console.error(`❌ Failed to create error log file: ${err.message}`);
    }
  }
}

function logError(message, filePath) {
  try {
    // Use the full file path instead of just the filename
    if (!existsSync('error_transcode.txt')) {
      // Create file with header if it doesn't exist
      const timestamp = new Date().toISOString();
      const header = `Transcoding Error Log - Created at ${timestamp}\n`;
      writeFileSync('error_transcode.txt', header);
    }

    // Just write the full file path, one per line
    appendFileSync('error_transcode.txt', `${filePath}\n`);
    console.error(`❌ Added to error log: ${filePath}`);
  } catch (err) {
    console.error('Failed to write to error log:', err);
  }
}

async function checkFileExistsInDrive(drive, folderId, fileName) {
  try {
    const response = await drive.files.list({
      q: `'${folderId}' in parents and name='${fileName}' and trashed=false`,
      fields: 'files(id, name)',
      spaces: 'drive',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });
    console.log('🚀 ~ checkFileExistsInDrive ~ response:', response);

    return response.data.files && response.data.files.length > 0;
  } catch (error) {
    console.error(`Error checking if file exists in Drive: ${error.message}`);
    return false; // If there's an error, assume file doesn't exist
  }
}

async function findOrCreateFolder(drive, parentFolderId, folderName) {
  try {
    // First try to find if folder already exists
    const response = await drive.files.list({
      q: `mimeType='application/vnd.google-apps.folder' and name='${folderName}' and '${parentFolderId}' in parents and trashed=false`,
      fields: 'files(id,name)',
      spaces: 'drive',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });
    console.log('🚀 ~ findOrCreateFolder ~ response:', response);

    // If folder found, return its ID
    if (response.data.files && response.data.files.length > 0) {
      console.log(`✅ Found existing folder: ${folderName} (${response.data.files[0].id})`);
      return response.data.files[0].id;
    }

    // If not found, create new folder
    const fileMetadata = {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentFolderId],
    };

    const folder = await drive.files.create({
      resource: fileMetadata,
      fields: 'id,name',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    console.log(`✅ Created new folder: ${folderName} (${folder.data.id})`);
    return folder.data.id;
  } catch (error) {
    console.error(`❌ Error finding/creating folder ${folderName}:`, error.message);
    return null;
  }
}

async function verifyGoogleDriveFolder(drive, folderId) {
  try {
    const response1 = await drive.files.list({
      q: `'${folderId}' in parents and trashed=false`,
      fields: 'files(id, name, mimeType, parents)',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });
    console.log(response1.data.files);

    // Use files.get instead of drives.get
    const response = await drive.files.get({
      fileId: folderId,
      fields: 'name,id,mimeType',
      supportsAllDrives: true,
    });

    // Check if it's actually a folder
    if (response.data.mimeType !== 'application/vnd.google-apps.folder') {
      console.error(`The ID ${folderId} is not a folder. It's a ${response.data.mimeType}`);
      return false;
    }

    console.log(`✅ Google Drive folder verified: ${response.data.name} (${response.data.id})`);
    return true;
  } catch (error) {
    console.error(`❌ Error verifying Google Drive folder: ${error.message}`);

    if (error.message.includes('insufficient permission')) {
      console.error(`The service account doesn't have permission to access this folder.`);
      console.error(`Please share the folder with your service account email.`);
    }

    return false;
  }
}

async function setupGoogleDrive() {
  try {
    // Read OAuth credentials from file
    const content = await readFile('oauth-credentials.json');
    const credentials = JSON.parse(content);
    console.log(
      `Using OAuth client: ${
        credentials.web ? credentials.web.client_id : credentials.installed.client_id
      }`
    );

    const oauth2Client = new google.auth.OAuth2(
      credentials.web ? credentials.web.client_id : credentials.installed.client_id,
      credentials.web ? credentials.web.client_secret : credentials.installed.client_secret,
      credentials.web ? credentials.web.redirect_uris[0] : credentials.installed.redirect_uris[0]
    );

    // Check if we have stored tokens
    let tokens;
    try {
      const tokenContent = await readFile('oauth-tokens.json');
      tokens = JSON.parse(tokenContent);
      console.log('Found stored OAuth tokens');
    } catch (err) {
      console.log('No stored tokens found, need to authorize');
      tokens = await getNewTokens(oauth2Client);
    }

    // Set the credentials
    oauth2Client.setCredentials(tokens);

    // Save refresh token workflow if needed
    oauth2Client.on('tokens', (newTokens) => {
      if (newTokens.refresh_token) {
        // Store new tokens
        const updatedTokens = { ...tokens, ...newTokens };
        fs.writeFileSync('oauth-tokens.json', JSON.stringify(updatedTokens, null, 2));
        console.log('New OAuth tokens saved');
      }
    });

    return google.drive({ version: 'v3', auth: oauth2Client });
  } catch (error) {
    console.error('Error setting up Google Drive:', error);
    return null;
  }
}

async function getNewTokens(oauth2Client) {
  return new Promise((resolve, reject) => {
    // Generate the authorization URL
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: ['https://www.googleapis.com/auth/drive'],
    });

    console.log('Authorize this app by visiting this URL:', authUrl);

    // Set up readline interface to get authorization code from user
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    rl.question('Enter the code from that page here: ', async (code) => {
      rl.close();
      try {
        // Exchange auth code for tokens
        const { tokens } = await oauth2Client.getToken(code);
        console.log('OAuth tokens obtained successfully');

        // Save tokens to file for future use
        fs.writeFileSync('oauth-tokens.json', JSON.stringify(tokens, null, 2));
        console.log('OAuth tokens saved to oauth-tokens.json');

        resolve(tokens);
      } catch (err) {
        console.error('Error retrieving access token:', err);
        reject(err);
      }
    });
  });
}

async function trashFile(drive, fileId) {
  try {
    await drive.files.update({
      fileId: fileId,
      resource: { trashed: true },
      supportsAllDrives: true,
    });
    return true;
  } catch (error) {
    console.error(`Error trashing file ${fileId}: ${error.message}`);
    return false;
  }
}

async function batchTrashFiles(drive, fileIds) {
  if (fileIds.length === 0) return { success: 0, notFound: 0, permissionDenied: 0 };

  const results = {
    success: 0,
    notFound: 0,
    permissionDenied: 0,
    otherErrors: 0,
  };

  // Process in batches of 10 to avoid rate limits
  const batchSize = 10;
  for (let i = 0; i < fileIds.length; i += batchSize) {
    const batch = fileIds.slice(i, i + batchSize);

    // Show progress
    console.log(
      `🗑️ Trashing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
        fileIds.length / batchSize
      )} (${i + 1}-${Math.min(i + batchSize, fileIds.length)}/${fileIds.length})`
    );

    const promises = batch.map((fileId) =>
      drive.files
        .update({
          fileId,
          resource: { trashed: true },
          supportsAllDrives: true,
        })
        .then(() => ({ status: 'success', fileId }))
        .catch((error) => {
          if (error.message.includes('File not found')) {
            return { status: 'notFound', fileId };
          } else if (error.message.includes('permission')) {
            return { status: 'permissionDenied', fileId };
          } else {
            return { status: 'error', fileId, error: error.message };
          }
        })
    );

    const batchResults = await Promise.all(promises);

    for (const result of batchResults) {
      switch (result.status) {
        case 'success':
          results.success++;
          console.log(`✅ Trashed file ID: ${result.fileId}`);
          break;
        case 'notFound':
          results.notFound++;
          console.log(`⚠️ File not found: ${result.fileId}`);
          break;
        case 'permissionDenied':
          results.permissionDenied++;
          console.error(`⚠️ Permission denied for file ${result.fileId}`);
          break;
        default:
          results.otherErrors++;
          console.error(`❌ Error trashing file ${result.fileId}: ${result.error}`);
      }
    }

    // Small delay between batches to avoid rate limits
    if (i + batchSize < fileIds.length) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }

  return results;
}

async function batchDeleteFiles(drive, fileIds) {
  if (fileIds.length === 0) return { success: 0, notFound: 0, permissionDenied: 0 };

  const results = {
    success: 0,
    notFound: 0,
    permissionDenied: 0,
  };

  // Process in batches of 10 to avoid rate limits
  const batchSize = 10;
  for (let i = 0; i < fileIds.length; i += batchSize) {
    const batch = fileIds.slice(i, i + batchSize);
    const promises = batch.map((fileId) =>
      drive.files
        .delete({
          fileId,
          supportsAllDrives: true,
        })
        .then(() => ({ status: 'success', fileId }))
        .catch((error) => {
          if (error.message.includes('File not found')) {
            return { status: 'notFound', fileId };
          } else if (error.message.includes('permission')) {
            return { status: 'permissionDenied', fileId };
          } else {
            return { status: 'error', fileId, error: error.message };
          }
        })
    );

    const batchResults = await Promise.all(promises);

    for (const result of batchResults) {
      switch (result.status) {
        case 'success':
          results.success++;
          break;
        case 'notFound':
          results.notFound++;
          break;
        case 'permissionDenied':
          results.permissionDenied++;
          break;
        default:
          console.error(`Error with file ${result.fileId}: ${result.error}`);
      }
    }

    // Small delay between batches to avoid rate limits
    if (i + batchSize < fileIds.length) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }

  return results;
}

async function uploadToGoogleDrive(drive, filePath, folderId) {
  if (!drive) return null;

  try {
    const fileMetadata = {
      name: path.basename(filePath),
      parents: [folderId],
    };

    const media = {
      mimeType: 'video/mp4',
      body: fs.createReadStream(filePath),
    };

    const response = await drive.files.create({
      resource: fileMetadata,
      media: media,
      fields: 'id,webViewLink',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    console.log(`Uploaded to Google Drive: ${response.data.webViewLink}`);
    return response.data;
  } catch (error) {
    console.error('Error uploading to Google Drive:', error);
    return null;
  }
}

async function checkFileExistsInS3(client, bucket, key) {
  try {
    await client.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: key,
      })
    );
    return true;
  } catch (error) {
    if (error.name === 'NotFound') {
      return false;
    }
    throw error;
  }
}

async function streamToFile(stream, filePath) {
  const directory = path.dirname(filePath);
  // Check if directory exists and create it if not
  try {
    await access(directory);
  } catch {
    await mkdir(directory, { recursive: true });
  }
  return new Promise((resolve, reject) => {
    const writeStream = createWriteStream(`${filePath}`);
    stream.pipe(writeStream);
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });
}

// New function to run FFmpeg with progress monitoring
function runFFmpegWithProgress(inputFile, outputFile, useCPU) {
  return new Promise((resolve, reject) => {
    const codecParam = useCPU ? 'libx264' : 'h264_nvenc';

    // Simplified arguments without hardware acceleration
    const ffmpegArgs = ['-c:v', 'hevc', '-i', inputFile, '-c:v', codecParam, '-y', outputFile];

    console.log(`🔄 Using encoder: ${codecParam}`);
    console.log(`🎬 FFmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

    const ffmpeg = spawn('ffmpeg', ffmpegArgs);

    let duration = 0;
    let currentTime = 0;
    let lastProgressUpdate = Date.now();
    let frameCount = 0;
    let fps = 0;
    let speed = 0;

    // Process FFmpeg output
    ffmpeg.stderr.on('data', (data) => {
      const output = data.toString();

      // Extract duration if we don't have it yet
      if (duration === 0) {
        const durationMatch = output.match(/Duration: (\d{2}):(\d{2}):(\d{2}.\d{2})/);
        if (durationMatch) {
          const hours = parseInt(durationMatch[1]);
          const minutes = parseInt(durationMatch[2]);
          const seconds = parseFloat(durationMatch[3]);
          duration = hours * 3600 + minutes * 60 + seconds;
          console.log(`📏 Video duration: ${formatTime(duration)}`);
        }
      }

      // Extract current time from stderr
      const timeMatch = output.match(/time=(\d{2}):(\d{2}):(\d{2}.\d{2})/);
      if (timeMatch) {
        const hours = parseInt(timeMatch[1]);
        const minutes = parseInt(timeMatch[2]);
        const seconds = parseFloat(timeMatch[3]);
        currentTime = hours * 3600 + minutes * 60 + seconds;
      }

      // Extract frame information from stderr
      const frameMatch = output.match(/frame=\s*(\d+)/);
      if (frameMatch) {
        frameCount = parseInt(frameMatch[1]);
      }

      // Extract fps information
      const fpsMatch = output.match(/(\d+\.?\d*) fps/);
      if (fpsMatch) {
        fps = parseFloat(fpsMatch[1]);
      }

      // Extract speed information
      const speedMatch = output.match(/speed=(\d+\.?\d*)x/);
      if (speedMatch) {
        speed = parseFloat(speedMatch[1]);
      }

      // Update progress every second
      const now = Date.now();
      if (now - lastProgressUpdate > 1000 && duration > 0 && currentTime > 0) {
        lastProgressUpdate = now;
        const progress = Math.min(100, Math.round((currentTime / duration) * 100));
        const remainingTime = duration - currentTime;
        const estimatedTimeLeft = speed > 0 ? remainingTime / speed : remainingTime;

        console.log(
          `
    📊 FFmpeg Progress:
       ▶️ ${progress}% complete (${formatTime(currentTime)}/${formatTime(duration)})
       🖼️ Frame: ${frameCount}${fps > 0 ? `, FPS: ${fps.toFixed(1)}` : ''}
       ⏱️ Speed: ${speed.toFixed(2)}x
       🕒 ETA: ${formatTime(estimatedTimeLeft)}
          `.trim()
        );
      }
    });

    // Process FFmpeg progress info
    ffmpeg.stdout.on('data', (data) => {
      // Just in case ffmpeg outputs anything to stdout without the progress pipe
    });

    ffmpeg.on('close', (code) => {
      if (code === 0) {
        console.log('✅ Transcoding completed successfully (100%)');
        resolve();
      } else {
        reject(new Error(`FFmpeg exited with code ${code}`));
      }
    });

    ffmpeg.on('error', (err) => {
      reject(new Error(`FFmpeg process error: ${err.message}`));
    });
  });
}

// Format time in seconds to HH:MM:SS
function formatTime(seconds) {
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs
    .toString()
    .padStart(2, '0')}`;
}

async function processFile(client, drive, INPUT_KEY, fileIndex, totalFiles) {
  try {
    console.log(`\n🔄 Processing file ${fileIndex}/${totalFiles}: ${INPUT_KEY}`);

    const keyPath = INPUT_KEY.slice(0, INPUT_KEY.lastIndexOf('.'));
    const convertedKey = `${keyPath}_converted.mp4`;
    const fileName = path.basename(convertedKey);
    console.log(`🚀 Output file: ${convertedKey}`);

    // Check if converted file already exists in S3
    const convertedExists = await checkFileExistsInS3(client, BUCKET, convertedKey);
    if (convertedExists) {
      console.log(`⏭️ Skip: ${convertedKey} already exists in S3`);
      return;
    }

    // Check if file exists in Google Drive
    if (drive) {
      const cameraFolder = extractCameraId(INPUT_KEY);
      console.log(`📂 Checking camera folder: ${cameraFolder}`);

      // Find or get the camera folder ID
      const cameraFolderId = await findOrCreateFolder(drive, GOOGLE_DRIVE_FOLDER_ID, cameraFolder);

      if (cameraFolderId) {
        const fileExists = await checkFileExistsInDrive(drive, cameraFolderId, fileName);
        if (fileExists) {
          console.log(`⏭️ Skip: ${fileName} already exists in Google Drive folder ${cameraFolder}`);
          if (existsSync(INPUT_KEY)) {
            await unlink(INPUT_KEY);
            console.log(`✅ Deleted local file: ${INPUT_KEY}`);
          }
          if (existsSync(convertedKey)) {
            await unlink(convertedKey);
            console.log(`✅ Deleted local file: ${convertedKey}`);
          }
          console.log(`✅ Deleted local file: ${convertedKey}`);
          logError(`File already exists in Google Drive: ${fileName}`, INPUT_KEY);
          return;
        }
      }
    }

    let downloadedFile = false;
    // Download the file if needed
    if (!existsSync(INPUT_KEY)) {
      console.log(`⬇️ Downloading: ${INPUT_KEY} from S3`);
      try {
        const { Body } = await client.send(
          new GetObjectCommand({
            Bucket: BUCKET,
            Key: INPUT_KEY,
          })
        );
        await streamToFile(Body, INPUT_KEY);
        console.log('✅ Downloaded S3 file successfully');
        downloadedFile = true;
      } catch (downloadError) {
        console.error(`❌ Download error: ${downloadError.message}`);
        logError(`Download error: ${downloadError.message}`, INPUT_KEY);
        return;
      }
    } else {
      console.log(`📁 Using existing local file: ${INPUT_KEY}`);
    }

    let transcodeSuccess = false;
    // Transcode the video with progress monitoring
    console.log(`🎬 Starting transcoding: ${INPUT_KEY}`);
    try {
      // Use the new function with progress reporting
      await runFFmpegWithProgress(INPUT_KEY, convertedKey, useCPU);
      transcodeSuccess = true;

      // Upload to Google Drive
      if (drive) {
        try {
          // Extract camera ID for folder name
          const cameraFolder = extractCameraId(INPUT_KEY);
          console.log(`📂 Using camera folder: ${cameraFolder}`);

          // Find or create the camera folder under the parent folder
          const cameraFolderId = await findOrCreateFolder(
            drive,
            GOOGLE_DRIVE_FOLDER_ID,
            cameraFolder
          );

          if (cameraFolderId) {
            console.log(`⬆️ Uploading to Google Drive folder: ${cameraFolder}`);
            await uploadToGoogleDrive(drive, convertedKey, cameraFolderId);
            console.log(`✅ Uploaded to Google Drive: ${convertedKey}`);
          } else {
            console.error('❌ Could not find or create camera folder, uploading to parent folder');
            logError('Could not find or create camera folder', INPUT_KEY);
            await uploadToGoogleDrive(drive, convertedKey, GOOGLE_DRIVE_FOLDER_ID);
          }
        } catch (driveError) {
          console.error('❌ Google Drive upload failed:', driveError);
          logError(`Google Drive upload failed: ${driveError.message}`, INPUT_KEY);
        }
      }
    } catch (transcodeError) {
      console.error(`❌ Transcode error: ${transcodeError}`);
      logError(`Transcode error: ${transcodeError.message}`, INPUT_KEY);
    } finally {
      // Clean up local files regardless of success or failure
      console.log('🧹 Cleaning up local files');
      try {
        // Only delete input file if we downloaded it or transcoding succeeded
        if (downloadedFile || transcodeSuccess) {
          if (existsSync(INPUT_KEY)) {
            await unlink(INPUT_KEY);
            console.log(`✅ Deleted input file: ${INPUT_KEY}`);
          }
        }

        // Delete output file if it exists (regardless of success)
        if (existsSync(convertedKey)) {
          await unlink(convertedKey);
          console.log(`✅ Deleted output file: ${convertedKey}`);
        }
      } catch (unlinkError) {
        console.error('❌ Failed to delete local files:', unlinkError);
        logError(`Failed to delete local files: ${unlinkError.message}`, INPUT_KEY);
      }
    }
  } catch (error) {
    console.error('❌ Error processing file:', INPUT_KEY, error);
    logError(`Error processing file: ${error.message}`, INPUT_KEY);
  }
}

async function transcodeFile() {
  await initErrorLog();

  const client = new S3Client({ region: REGION });
  let drive = null;

  try {
    // Set up Google Drive
    drive = await setupGoogleDrive();
    if (drive) {
      console.log('✅ Google Drive API initialized successfully');

      // Verify folder access
      const folderVerified = await verifyGoogleDriveFolder(drive, GOOGLE_DRIVE_FOLDER_ID);
      if (!folderVerified) {
        console.error(
          '❌ Google Drive folder access failed. Continuing without Google Drive upload.'
        );
        drive = null;
      }
      if (scanForDuplicates) {
        console.log('\n🔍 Starting Google Drive duplicate file scan...');
        const duplicateMap = await scanGoogleDriveForDuplicates(drive, GOOGLE_DRIVE_FOLDER_ID);

        if (duplicateMap) {
          await handleDuplicates(drive, duplicateMap);
          console.log('✅ Duplicate handling completed');
        } else {
          console.log('✅ No duplicates found, nothing to handle');
        }
        return; // Exit after scanning
      }
    }
  } catch (error) {
    console.warn('⚠️ Google Drive API initialization failed:', error.message);
  }

  try {
    // Read the ID list file
    console.log('📄 Reading file list');
    const fileContent = await readFile('id_list.txt', 'utf-8');
    const allFiles = fileContent
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    // Filter files based on instance parameters
    const keyListToProcess = allFiles.filter(
      (_, index) => index % totalInstances === currentInstance
    );

    console.log(`📋 Total files found: ${allFiles.length}`);
    console.log(
      `📋 This instance (${currentInstance}) will process: ${keyListToProcess.length} files`
    );

    // Process each file one by one with file number tracking
    for (let i = 0; i < keyListToProcess.length; i++) {
      const fileNumber = i + 1;
      await processFile(client, drive, keyListToProcess[i], fileNumber, keyListToProcess.length);
    }

    console.log('✅ All processing completed for instance', currentInstance);
  } catch (error) {
    console.error('❌ Error reading id_list.txt:', error);
  }
}

transcodeFile();
