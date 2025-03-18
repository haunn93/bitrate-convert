import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { exec, spawn } from 'child_process';
import { appendFileSync, createWriteStream, existsSync, writeFileSync } from 'fs';
import { mkdir, access, readFile, unlink } from 'fs/promises';
import * as path from 'path';
import { google } from 'googleapis';
import fs from 'fs';
import util from 'util';
import readline from 'readline';

//command line arguments
const args = process.argv.slice(2);
const currentInstance = parseInt(args[0] || '0', 10); // Default to 0 if not specified
const totalInstances = parseInt(args[1] || '1', 10); // Default to 1 if not specified
const useCPU = args[2] === 'cpu'; // Use 'cpu' as third parameter to use CPU encoding

console.log(`🔢 Running as instance ${currentInstance} of ${totalInstances}`);
console.log(`🖥️ Using ${useCPU ? 'CPU (libx264)' : 'GPU (h264_nvenc)'} for encoding`);

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

// New function to run FFmpeg with progress monitoring
function runFFmpegWithProgress(inputFile, outputFile, useCPU) {
  return new Promise((resolve, reject) => {
    const codecParam = useCPU ? 'libx264' : 'h264_nvenc';

    // Simplified arguments without hardware acceleration
    const ffmpegArgs = [
      '-c:v',
      'hevc',
      '-i',
      inputFile,
      '-c:v',
      codecParam,
      '-progress',
      'pipe:1',
      '-y',
      outputFile,
    ];

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
    });

    // Process FFmpeg progress info
    ffmpeg.stdout.on('data', (data) => {
      const output = data.toString();

      // Extract current time
      const timeMatch = output.match(/out_time=(\d{2}):(\d{2}):(\d{2}.\d{2})/);
      if (timeMatch) {
        const hours = parseInt(timeMatch[1]);
        const minutes = parseInt(timeMatch[2]);
        const seconds = parseFloat(timeMatch[3]);
        currentTime = hours * 3600 + minutes * 60 + seconds;

        // Extract frame information
        const frameMatch = output.match(/frame=(\d+)/);
        if (frameMatch) {
          frameCount = parseInt(frameMatch[1]);
        }

        // Update progress every second
        const now = Date.now();
        if (now - lastProgressUpdate > 1000 && duration > 0) {
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
      }
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

    // Transcode the video with progress monitoring
    console.log(`🎬 Starting transcoding: ${INPUT_KEY}`);
    try {
      // Use the new function with progress reporting
      await runFFmpegWithProgress(INPUT_KEY, convertedKey, useCPU);

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
