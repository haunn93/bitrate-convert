import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import dotenv from 'dotenv';
import { appendFileSync, createReadStream, createWriteStream, existsSync, writeFileSync } from 'fs';
import { access, mkdir, readFile, unlink } from 'fs/promises';
import { google } from 'googleapis';
import * as path from 'path';
import readline from 'readline';
import { fileTypeFromFile } from 'file-type';
dotenv.config();

// Command line arguments
const args = process.argv.slice(2);
const currentInstance = parseInt(args[0] || '0', 10);
const totalInstances = parseInt(args[1] || '1', 10);

console.log(`🔢 Running as instance ${currentInstance} of ${totalInstances}`);

const BUCKET = process.env.BUCKET;
const REGION = process.env.REGION;
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;

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
      const timestamp = new Date().toISOString();
      const header = `Transfer Error Log - Created at ${timestamp}\n`;
      writeFileSync(errorLogFile, header);
      console.log(`📝 Created error log file: ${errorLogFile}`);
    } catch (err) {
      console.error(`❌ Failed to create error log file: ${err.message}`);
    }
  }
}

function logError(message, filePath) {
  try {
    if (!existsSync('error_transcode.txt')) {
      const timestamp = new Date().toISOString();
      const header = `Transfer Error Log - Created at ${timestamp}\n`;
      writeFileSync('error_transcode.txt', header);
    }

    appendFileSync('error_transcode.txt', `${filePath}\n`);
    console.error(`❌ Added to error log: ${filePath}`);
  } catch (err) {
    console.error('Failed to write to error log:', err);
  }
}

async function streamToFile(stream, filePath) {
  const directory = path.dirname(filePath);
  try {
    await access(directory);
  } catch {
    await mkdir(directory, { recursive: true });
  }

  return new Promise((resolve, reject) => {
    const writeStream = createWriteStream(filePath);
    let totalBytes = 0;
    let downloadedBytes = 0;
    let lastLogTime = Date.now();
    const logInterval = 500;

    if (stream.headers && stream.headers['content-length']) {
      totalBytes = parseInt(stream.headers['content-length']);
    }

    stream.on('data', (chunk) => {
      downloadedBytes += chunk.length;
      const currentTime = Date.now();

      if (currentTime - lastLogTime >= logInterval) {
        if (totalBytes > 0) {
          const progress = ((downloadedBytes / totalBytes) * 100).toFixed(1);
          const downloaded = (downloadedBytes / (1024 * 1024)).toFixed(1);
          const total = (totalBytes / (1024 * 1024)).toFixed(1);
          process.stdout.write(`\r⬇️ Download: ${progress}% (${downloaded}MB / ${total}MB)`);
        } else {
          const downloaded = (downloadedBytes / (1024 * 1024)).toFixed(1);
          process.stdout.write(`\r⬇️ Downloaded: ${downloaded}MB`);
        }
        lastLogTime = currentTime;
      }
    });

    stream.pipe(writeStream);

    writeStream.on('finish', () => {
      const finalSize = (downloadedBytes / (1024 * 1024)).toFixed(1);
      process.stdout.write(`\r✅ Download complete: ${finalSize}MB total\n`);
      resolve();
    });

    writeStream.on('error', reject);
    stream.on('error', reject);
  });
}

async function setupGoogleDrive() {
  try {
    const content = await readFile('oauth-credentials.json');
    const credentials = JSON.parse(content);

    const oauth2Client = new google.auth.OAuth2(
      credentials.web ? credentials.web.client_id : credentials.installed.client_id,
      credentials.web ? credentials.web.client_secret : credentials.installed.client_secret,
      credentials.web ? credentials.web.redirect_uris[0] : credentials.installed.redirect_uris[0]
    );

    let tokens;
    try {
      const tokenContent = await readFile('oauth-tokens.json');
      tokens = JSON.parse(tokenContent);
      console.log('Found stored OAuth tokens');
    } catch (err) {
      console.log('No stored tokens found, need to authorize');
      tokens = await getNewTokens(oauth2Client);
    }

    oauth2Client.setCredentials(tokens);
    oauth2Client.on('tokens', (newTokens) => {
      if (newTokens.refresh_token) {
        const updatedTokens = { ...tokens, ...newTokens };
        writeFileSync('oauth-tokens.json', JSON.stringify(updatedTokens, null, 2));
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
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: ['https://www.googleapis.com/auth/drive'],
    });

    console.log('Authorize this app by visiting this URL:', authUrl);

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    rl.question('Enter the code from that page here: ', async (code) => {
      rl.close();
      try {
        const { tokens } = await oauth2Client.getToken(code);
        console.log('OAuth tokens obtained successfully');
        writeFileSync('oauth-tokens.json', JSON.stringify(tokens, null, 2));
        console.log('OAuth tokens saved to oauth-tokens.json');
        resolve(tokens);
      } catch (err) {
        console.error('Error retrieving access token:', err);
        reject(err);
      }
    });
  });
}

async function findOrCreateFolder(drive, parentFolderId, folderName) {
  try {
    const response = await drive.files.list({
      q: `mimeType='application/vnd.google-apps.folder' and name='${folderName}' and '${parentFolderId}' in parents and trashed=false`,
      fields: 'files(id,name)',
      spaces: 'drive',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    if (response.data.files && response.data.files.length > 0) {
      console.log(`✅ Found existing folder: ${folderName}`);
      return response.data.files[0].id;
    }

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

    console.log(`✅ Created new folder: ${folderName}`);
    return folder.data.id;
  } catch (error) {
    console.error(`❌ Error finding/creating folder ${folderName}:`, error.message);
    return null;
  }
}

async function uploadToGoogleDrive(drive, filePath, folderId) {
  if (!drive) return null;

  try {
    const fileType = await fileTypeFromFile(filePath);
    const mimeType = fileType?.mime || 'video/mp4'; // fallback to video/mp4 if detection fails

    const fileMetadata = {
      name: path.basename(filePath),
      parents: [folderId],
    };

    const media = {
      mimeType: mimeType,
      body: createReadStream(filePath),
    };

    const response = await drive.files.create({
      resource: fileMetadata,
      media: media,
      fields: 'id,webViewLink',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    console.log(`✅ Uploaded to Google Drive: ${response.data.webViewLink}`);
    return response.data;
  } catch (error) {
    console.error('❌ Error uploading to Google Drive:', error);
    return null;
  }
}

async function processFile(client, drive, INPUT_KEY, fileIndex, totalFiles) {
  try {
    console.log(`\n🔄 Processing file ${fileIndex}/${totalFiles}: ${INPUT_KEY}`);
    const cameraFolder = extractCameraId(INPUT_KEY);
    console.log(`📂 Using camera folder: ${cameraFolder}`);

    const cameraFolderId = await findOrCreateFolder(drive, GOOGLE_DRIVE_FOLDER_ID, cameraFolder);
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
    // Download from S3
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
      } catch (downloadError) {
        console.error(`❌ Download error: ${downloadError.message}`);
        logError(`Download error: ${downloadError.message}`, INPUT_KEY);
        return;
      }
    } else {
      console.log(`📁 Using existing local file: ${INPUT_KEY}`);
    }

    // Upload to Google Drive
    if (drive) {
      try {
        if (cameraFolderId) {
          console.log(`⬆️ Uploading to Google Drive folder: ${cameraFolder}`);
          await uploadToGoogleDrive(drive, INPUT_KEY, cameraFolderId);
        } else {
          console.error('❌ Could not find or create camera folder, uploading to parent folder');
          await uploadToGoogleDrive(drive, INPUT_KEY, GOOGLE_DRIVE_FOLDER_ID);
        }
      } catch (driveError) {
        console.error('❌ Google Drive upload failed:', driveError);
        logError(`Google Drive upload failed: ${driveError.message}`, INPUT_KEY);
      }
    }

    // Clean up local file
    try {
      if (existsSync(INPUT_KEY)) {
        await unlink(INPUT_KEY);
        console.log(`✅ Deleted local file: ${INPUT_KEY}`);
      }
    } catch (unlinkError) {
      console.error('❌ Failed to delete local file:', unlinkError);
    }
  } catch (error) {
    console.error('❌ Error processing file:', INPUT_KEY, error);
    logError(`Error processing file: ${error.message}`, INPUT_KEY);
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

    return response.data.files && response.data.files.length > 0;
  } catch (error) {
    console.error(`Error checking if file exists in Drive: ${error.message}`);
    return false; // If there's an error, assume file doesn't exist
  }
}

async function main() {
  await initErrorLog();

  const client = new S3Client({ region: REGION });
  let drive = null;

  try {
    drive = await setupGoogleDrive();
    if (drive) {
      console.log('✅ Google Drive API initialized successfully');
    }
  } catch (error) {
    console.warn('⚠️ Google Drive API initialization failed:', error.message);
    return;
  }

  try {
    console.log('📄 Reading file list');
    const fileContent = await readFile('id_list.txt', 'utf-8');
    const allFiles = fileContent
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    const keyListToProcess = allFiles.filter(
      (_, index) => index % totalInstances === currentInstance
    );

    console.log(`📋 Total files found: ${allFiles.length}`);
    console.log(
      `📋 This instance (${currentInstance}) will process: ${keyListToProcess.length} files`
    );

    for (let i = 0; i < keyListToProcess.length; i++) {
      await processFile(client, drive, keyListToProcess[i], i + 1, keyListToProcess.length);
    }

    console.log('✅ All processing completed for instance', currentInstance);
  } catch (error) {
    console.error('❌ Error reading id_list.txt:', error);
  }
}

main();
