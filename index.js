const Parser = require('rss-parser');
const parser = new Parser();
const fetch = require('node-fetch');
const ffmpeg = require('fluent-ffmpeg');
const AWS = require("aws-sdk");
const fs = require("fs");
const Podcast = require('podcast');
const SpeechToTextV1 = require('ibm-watson/speech-to-text/v1');
const { IamAuthenticator } = require('ibm-watson/auth');
require('dotenv').config();

// Define the path for the DO Space
const space_url = `https://${process.env.DO_SPACES_NAME}.${process.env.DO_SPACES_ENDPOINT}`;



/**
 *    url - string, path of file to download
 *    path - string, path of output file     
 */
async function download (url, path) {
	console.log('Downloading File...');
	const res = await fetch(url);
	const fileStream = fs.createWriteStream(path);
	await new Promise((resolve, reject) => {
		res.body.pipe(fileStream);
		res.body.on("error", reject);
		fileStream.on("finish", resolve);
	});
};


/**
 *    input - string, path of input file
 *    output - string, path of output file
 *    callback - function, fn (error, result)        
 */
async function convert(input, output) {
	await new Promise((resolve, reject) => {
		try {
			ffmpeg(input)
				.noVideo()
				.output(output)
				.on('progress', function(progress) {
					console.log('Processing: ' + progress.percent + '% done');
				})
				.on('end', function() {                    
					console.log('conversion ended');
					resolve(output);
				}).on('error', function(err){
					console.log('error: ', err.code, err.msg);
					throw new Error(err)
				}).run();
		} catch (error) {
			console.log(error);
			reject(error);
		}
	});
}


/**
 *    input - string, path of input file
 *    output - string, path of output file
 *    callback - function, fn (error, result)    
 */
async function upload(input, output) {
	console.log('Uploading File...');
	const spacesEndpoint = new AWS.Endpoint(process.env.DO_SPACES_ENDPOINT);
	const s3 = new AWS.S3({
		endpoint: spacesEndpoint, 
		accessKeyId: process.env.DO_SPACES_KEY, 
		secretAccessKey: process.env.DO_SPACES_SECRET
	});

	const file = fs.readFileSync(input);

	await new Promise((resolve, reject) => {
		try {
			s3.putObject({
				Bucket: process.env.DO_SPACES_NAME, 
				Key: output, 
				Body: file, 
				ACL: "public-read"}, (err, data) => {
				if (err) {
					throw new Error(err);
				} else {
					console.log("Your file has been uploaded successfully!", output);
					resolve(output);
				}
			});
		} catch (error) {
			console.log(error);
			reject(error)
		}
	});

}


/**
 * audio_file_path – string, path of the file to transcribe
 */
async function transcribe(audio_file_path) {
	console.log('Beginning Transcription...');

	// Authenticate
	const speechToText = new SpeechToTextV1({
		authenticator: new IamAuthenticator({
			apikey: process.env.IBM_API_KEY,
		}),
		serviceUrl: process.env.IBM_API_URL,
		disableSslVerification: true,
		maxContentLength: Infinity,
		maxBodyLength: Infinity
	});

	// Set the params for the transcription
	const recognizeParams = {
		audio: fs.createReadStream(audio_file_path),
		contentType: 'audio/mp3'
	};


	// Function to stitch the results together after.
	const stitch = (transcript, name) => {
		let results = transcript.result.results.map(r => r.alternatives[0].transcript);
		fs.writeFileSync(`${name}.txt`, results.join(''));
		console.log('Transcription Finished.');
	}

	// Begin the transcription.
	await new Promise((resolve, reject) => {
		try {
			speechToText.recognize(recognizeParams)
			.then(async speechRecognitionResults => {

				// Generate the filename for the transcript
				let split_a = audio_file_path.split('/');
				let transcript_file_name = split_a[split_a.length - 1].split('.')[0];

				// Output the full result of Speech-to-Text
				fs.writeFileSync(`${transcript_file_name}.json`, JSON.stringify(speechRecognitionResults, null, 2));

				// Create the plaintext transcript
				stitch(speechRecognitionResults, transcript_file_name);

				// Upload the transcripts
				await upload(`${transcript_file_name}.json`, `${transcript_file_name}.json`);
				await upload(`${transcript_file_name}.txt`, `${transcript_file_name}.txt`);

				resolve(transcript_file_name);
			})
			.catch(err => {
				console.log('error:', err);
				throw new Error(err);
			});
			
		} catch (error) {
			reject(error);
		}
	});
}

async function delete_file(video_file_name) {
	console.log('Deleting Video File...');
	fs.unlink(video_file_name, (err) => {
		if (err) {
			console.error(err)
		}
		return;
	});
}

function is_avail(video_link) {
	return video_link.includes('not-available') ? false : true;
}

function is_new(activity_id, pod_feed) {
	// Check here if we've already processed this video.
	if (!pod_feed.items[0]) return true;

	let link_split = pod_feed.items[0].guid.split('/');
	let latest_activity_id = link_split[link_split.length - 1];
	console.log({activity_id, latest_activity_id});
	return activity_id === latest_activity_id ? false : true;
}

function get_meeting_info(item, activity_id) {
	return {
		title: item.title,
		link: item.link,
		content: item.content,
		guid: item.guid,
		pubDate: item.pubDate,
		activity_id: activity_id,
		enclosure: {
			url: null,
			size: null
		},
		itunesExplicit: false,
		itunesSummary: item.content
	}
}

async function update_xml(current_feed, new_item, audio_file) {
	const feed = new Podcast(current_feed);
	const stats = fs.statSync(`./tmp/${audio_file}`);

	// Set up new episode file info
	new_item.enclosure.url = `${space_url}/${audio_file}`;
	new_item.enclosure.size = stats.size;

	// Add item to feed
	feed.addItem(new_item);

	// Write new xml file
	fs.writeFileSync('dcc_audio.xml', feed.buildXml('\t'));

	// Update existing feed
	upload('dcc_audio.xml', 'dcc_audio.xml');
}


(async () => {

	// Setup the RSS feeds
	let dcc_feed = await parser.parseURL('https://dublincity.public-i.tv/core/data/7844');
	
	let pod_feed = await parser.parseURL(`${space_url}/dcc_audio.xml`);
	
	for (const item of dcc_feed.items) {

		// Get the id of this particular meeting
		let link_split = item.link.split('/');
		let activity_id = link_split[link_split.length - 1];

		// Create the initial link (which redirects to the actual video).
		let redirect_link = `https://dublincity.public-i.tv/core/redirect/download_webcast/${activity_id}/video.mp4`;

		// Get the actual video link.
		let video_link = await fetch(redirect_link).then(res => res.url);

		// Create the file names.
		let video_file_name = `./tmp/${Date.now()}_${activity_id}.mp4`;
		let audio_file_name = `${Date.now()}_${activity_id}.mp3`;

		// Make sure the video is available
		if (!is_avail(video_link)) continue;

		// Check if the video is new
		if (!is_new(activity_id, pod_feed)) break;

		// Get the meeting info to include with the podcast episode.
		let meeting_info = get_meeting_info(item, activity_id);

		// Download => Convert => Upload => Update Feed => Delete Video => Transcribe
		await download(video_link, video_file_name);
		await convert(video_file_name, `./tmp/${audio_file_name}`);
		await upload(`./tmp/${audio_file_name}`, audio_file_name); 
		await update_xml(pod_feed, meeting_info, audio_file_name);
		await delete_file(video_file_name);
		return await transcribe(`./tmp/1616096362038_548471.mp3`);
	}
})();