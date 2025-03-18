import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { exec } from 'child_process';
import { createWriteStream, existsSync } from 'fs';
import { mkdir, access, readFile, unlink } from 'fs/promises';
import * as path from 'path';
import { google } from 'googleapis';
import fs from 'fs';
import util from 'util';

// Convert exec to Promise
const execPromise = util.promisify(exec);

const BUCKET = process.env.BUCKET || 'prod.kineticeye.io.s3.us-east-1.land-vdr';
console.log('🚀 ~ BUCKET:', BUCKET);
const REGION = process.env.REGION || 'us-east-1';
console.log('🚀 ~ REGION:', REGION);
const GOOGLE_DRIVE_FOLDER_ID =
  process.env.GOOGLE_DRIVE_FOLDER_ID || '1_NNhuK3dgEKfyzB_GVFUpAMteD4TZ7JQ';

function extractCameraId(filePath) {
  const matches = filePath.match(/camera-(\d+)/);
  if (matches && matches.length > 1) {
    return `camera-${matches[1]}`;
  }
  return 'other-videos'; // Default folder name if no camera ID found
}

function logError(message, filePath) {
  const timestamp = new Date().toISOString();
  const logEntry = `${timestamp} - ${filePath} - ${message}\n`;
  try {
    appendFileSync('error_transcode.txt', logEntry);
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
    });

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
    });

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
    });

    console.log(`✅ Created new folder: ${folderName} (${folder.data.id})`);
    return folder.data.id;
  } catch (error) {
    console.error(`❌ Error finding/creating folder ${folderName}:`, error.message);
    return null;
  }
}

// Add verification for Google Drive folder
async function verifyGoogleDriveFolder(drive, folderId) {
  try {
    // Use files.get instead of drives.get
    const response = await drive.files.get({
      fileId: folderId,
      fields: 'name,id,mimeType',
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
    // Read credentials from file
    const content = await readFile('google-credentials.json');
    const credentials = JSON.parse(content);
    console.log(`Using Google service account: ${credentials.client_email}`);

    // Create auth client from service account
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.metadata.readonly',
      ],
    });

    return google.drive({ version: 'v3', auth });
  } catch (error) {
    console.error('Error setting up Google Drive:', error);
    return null;
  }
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

async function processFile(client, drive, INPUT_KEY) {
  try {
    const keyPath = INPUT_KEY.slice(0, INPUT_KEY.lastIndexOf('.'));
    const convertedKey = `${keyPath}_converted.mp4`;
    const fileName = path.basename(convertedKey);
    console.log(`\n🔄 Processing: ${INPUT_KEY}`);
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
          return;
        }
      }
    }

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
      } catch (downloadError) {
        console.error(`❌ Download error: ${downloadError.message}`);
        logError(`Download error: ${downloadError.message}`, INPUT_KEY);
        return;
      }
    } else {
      console.log(`📁 Using existing local file: ${INPUT_KEY}`);
    }

    // Transcode the video
    console.log(`🎬 Starting transcoding: ${INPUT_KEY}`);
    try {
      const { stdout, stderr } = await execPromise(
        `ffmpeg -c:v hevc -i "${INPUT_KEY}" -c:v libx264 "${convertedKey}" -y`
      );
      console.log('✅ Transcoding completed successfully');

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

      // Clean up local files
      console.log('🧹 Cleaning up local files');
      try {
        await unlink(INPUT_KEY);
        await unlink(convertedKey);
        console.log('✅ Deleted local files');
      } catch (unlinkError) {
        console.error('❌ Failed to delete local files:', unlinkError);
        logError(`Failed to delete local files: ${unlinkError.message}`, INPUT_KEY);
      }
    } catch (transcodeError) {
      console.error(`❌ Transcode error: ${transcodeError}`);
      logError(`Transcode error: ${transcodeError.message}`, INPUT_KEY);
    }
  } catch (error) {
    console.error('❌ Error processing file:', INPUT_KEY, error);
    logError(`Error processing file: ${error.message}`, INPUT_KEY);
  }
}

async function transcodeFile() {
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
    }
  } catch (error) {
    console.warn('⚠️ Google Drive API initialization failed:', error.message);
  }

  try {
    // Read the ID list file
    console.log('📄 Reading file list');
    const fileContent = await readFile('id_list.txt', 'utf-8');
    const keyListToProcess = fileContent
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    console.log(`📋 Found ${keyListToProcess.length} files to process`);

    // Process each file one by one
    for (const INPUT_KEY of keyListToProcess) {
      await processFile(client, drive, INPUT_KEY);
    }

    console.log('✅ All processing completed');
  } catch (error) {
    console.error('❌ Error reading id_list.txt:', error);
  }
}

transcodeFile();
