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

const BUCKET = process.env.BUCKET || 'prod.kineticeye.io.s3.us-east-1.land-vdr';
console.log('🚀 ~ BUCKET:', BUCKET);
const REGION = process.env.REGION || 'us-east-1';
console.log('🚀 ~ REGION:', REGION);
const GOOGLE_DRIVE_FOLDER_ID =
  process.env.GOOGLE_DRIVE_FOLDER_ID || '1_NNhuK3dgEKfyzB_GVFUpAMteD4TZ7JQ'; // Replace with your shared folder ID

async function setupGoogleDrive() {
  try {
    // Read credentials from file
    const content = await readFile('google-credentials.json');
    const credentials = JSON.parse(content);

    // Create auth client from service account
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive.file'],
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

async function transcodeFile() {
  const client = new S3Client({ region: REGION });
  let drive = null;
  try {
    drive = await setupGoogleDrive();
    console.log('Google Drive API initialized successfully');
  } catch (error) {
    console.warn('Google Drive API initialization failed, continuing without it:', error.message);
  }
  try {
    // First read the ID list file
    const fileContent = await readFile('id_list.txt', 'utf-8');
    const keyListToProcess = fileContent
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id.length > 0); // Filter out empty values
    try {
      for (const INPUT_KEY of keyListToProcess) {
        const keyPath = INPUT_KEY.slice(0, INPUT_KEY.lastIndexOf('.'));
        const convertedKey = `${keyPath}_converted.mp4`;
        console.log('🚀 ~ convertedKey:', convertedKey);
        const convertedExists = await checkFileExistsInS3(client, BUCKET, convertedKey);
        if (convertedExists) {
          console.log(`Skip: ${convertedKey} already exists in S3`);
          continue;
        }
        try {
          // if (existsSync(convertedKey)) {
          //   console.log(`Skip: ${convertedKey} already exists locally`);
          //   continue;
          // }
          if (!existsSync(INPUT_KEY)) {
            console.log(`Downloading: ${INPUT_KEY} from S3`);
            const { Body } = await client.send(
              new GetObjectCommand({
                Bucket: BUCKET,
                Key: INPUT_KEY,
              })
            );
            await streamToFile(Body, INPUT_KEY);
            console.log('Downloaded S3 file successfully');
          } else {
            console.log(`Using existing local file: ${INPUT_KEY}`);
          }

          // Transcode the video
          exec(
            `ffmpeg -c:v hevc -i ${INPUT_KEY} -c:v libx264 ${convertedKey} -y`,
            async (error, stdout, stderr) => {
              if (error) {
                console.error(`Transcode error: ${error}`);
                return;
              }
              console.log('Transcoding completed successfully');
              console.log(`stdout: ${stdout}`);
              console.error(`stderr: ${stderr}`);
              //upload to Google Drive
              try {
                if (drive) {
                  await uploadToGoogleDrive(drive, convertedKey, GOOGLE_DRIVE_FOLDER_ID);
                  console.log(`Uploaded ${convertedKey} to Google Drive shared folder`);
                }
              } catch (driveError) {
                console.error('Google Drive upload failed:', driveError);
              }

              // Upload to S3
              // const putObject = new PutObjectCommand({
              //   Bucket: BUCKET,
              //   Key: convertedKey,
              //   Body: await readFile(`${convertedKey}`),
              // });
              // await client.send(putObject);
              // console.log(`Uploaded ${convertedKey} to S3`);

              // try {
              //   // Remove local files
              //   console.log('Deleting original file from local storage');
              //   await unlink(`${INPUT_KEY}`);
              //   console.log('Deleting original file from local storage');
              //   await unlink(`${convertedKey}`);
              //   console.log('Deleting transcoded file from local storage');
              // } catch (unlinkError) {
              //   console.error('Failed to delete local files:', unlinkError);
              // }
            }
          );
        } catch (error) {
          console.error('Error processing file:', INPUT_KEY, error);
        }
      }
    } catch (error) {
      console.error('Error reading id_list.example.txt:', error);
    }
  } catch (error) {
    console.error('Error processing files:', error);
  }
}

transcodeFile();
