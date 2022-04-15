import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import AWS from 'aws-sdk';
import axios from 'axios';
import { PassThrough } from 'stream';

dayjs.extend(utc);

const fileNames = [
  'buffalo-viewpoint-alberta',
  'genesee-alberta',
  'parry-sound-ontario',
  'st-dominique-st-montreal',
  'burlington-ontario',
  'gibbons-alberta',
  'peterborough-ontario',
  'steeper-alberta',
  '1050a-st-jean-baptiste-montreal',
  'calgary-central-inglewood-alberta',
  'grand-bend-ontario',
  'poplar-alberta',
  'st-lina-alberta',
  '12400-wilfrid-ouellette-montreal',
  'calgary-southeast-alberta',
  'grande-prairie-henry-pirker-alberta',
  'port-stanley-ontario',
  'stony-mountain-alberta',
  '20965-ste-marie-ste-anne-de-bellevue',
  'calgary-varsity-alberta',
  'grimshaw-alberta',
  'powers-alberta',
  'sudbury-ontario',
  '2495-chemin-duncan-mount-royal',
  'caroline-alberta',
  'guelph-ontario',
  'quebec-champigny-revolvair-exp',
  'surmont-2-alberta',
  '2580-boul-saint-joseph',
  'carrot-creek-alberta',
  'hamilton-downtown-ontario',
  'quebec-quebec-5445',
  'taber-airpointer-alberta',
  '3469958',
  'chatham-ontario',
  'hamilton-mountain-ontario',
  'quebec-secteur-de-champigny-5446',
  'tamarack-alberta',
  '4240-rue-charleroi-montreal-nord',
  'cold-lake-south-alberta',
  'hamilton-west-ontario',
  'quebec-secteur-du-vieux-limoilou-5447',
  'temiscaming-revolvair-exp',
  '7650-boulevard-chateauneuf-anjou',
  'conklin-community-alberta',
  'hinton-alberta',
  'quebec-vieux-limoilou-revolvair-exp',
  'terrebonne-parc-vaillant-revolvair-exp',
  'aeroport-de-montreal',
  'cornwall-ontario',
  'janvier-alberta',
  'red-deer-lancaster-alberta',
  'thunder-bay-ontario',
  'airdrie-alberta',
  'dorset-ontario',
  'kingston-ontario',
  'red-deer-riverside-alberta',
  'tiverton-ontario',
  'anzac-alberta',
  'drayton-valley-alberta',
  'kitchener-ontario',
  'redwater-alberta',
  'tomahawk-alberta',
  'ardrossan-alberta',
  'edmonton-east-alberta',
  'lamont-county-alberta',
  'rouyn-noranda-centre-ville-revolvair-exp',
  'toronto-downtown-ontario',
  'barrie-ontario',
  'edmonton-mccauley-alberta',
  'lanaudiere-quebec-5426',
  'rouyn-noranda-montee-du-sourire-revolvair-exp',
  'toronto-east-ontario',
  'beauce-quebec-canada-5438',
  'edmonton-south-alberta',
  'lethbridge-alberta',
  'saguenay-sainte-therese-arvida-revolvair-exp',
  'toronto-north-ontario',
  'beaverlodge-alberta',
  'edson-alberta',
  'levis-parc-georges-maranda-revolvair-exp',
  'saguenay-universite-chicoutimi-revolvair-exp',
  'toronto-west-ontario',
  'becancour-hotel-de-ville-revolvair-exp',
  'elk-island-alberta',
  'london-ontario',
  'sarnia-ontario',
  'town-of-lamont-alberta',
  'belleville-ontario',
  'ells-river-alberta',
  'medicine-hat-crescent-heights-alberta',
  'sault-ste-marie-ontario',
  'trois-rivieres-du-sanctuaire-revolvair-exp',
  'brampton-ontario',
  'fort-chipewyan-alberta',
  'milton-ontario',
  'shawinigan-saint-marc-revolvair-exp',
  'trois-rivieres-vieux-trois-rivieres-revolvair-exp',
  'brantford-ontario',
  'fort-mckay-alberta',
  'mississauga-ontario',
  'sherbrooke-secteur-est-revolvair-exp',
  'wapasu-alberta',
  'breton-alberta',
  'fort-mckay-south-alberta',
  'newmarket-ontario',
  'sherwood-park-alberta',
  'windsor-downtown-ontario',
  'brooks-airpointer-alberta',
  'fort-mcmurray-athabasca-valley-alberta',
  'north-bay-ontario',
  'smoky-heights-alberta',
  'windsor-west-ontario',
  'broomfield-station',
  'fort-mcmurray-patricia-mcinnes-alberta',
  'oakville-ontario',
  'sorel-tracy-saint-joseph-de-sorel-revolvair-exp',
  'woodcroft-alberta',
  'brossard-parc-sorbonne-revolvair-exp',
  'fort-saskatchewan-alberta',
  'oshawa-ontario',
  'st-albert-alberta',
  'bruderheim-alberta',
  'gatineau-hull-revolvair-exp',
  'ottawa-downtown-ontario',
  'st-catharines-ontario',
];

const s3 = new AWS.S3({ region: 'ca-central-1' });
const serverBase = 'https://app.revolvair.org/storage/archive-stations/';
// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const bucketName = process.env.S3BucketName!;

const migrateRevolvairToS3 = async (key: string) => {
  try {
    const endpoint = `${serverBase}${key}`;

    console.log(`Downloading file ${endpoint}`);
    const stream = await axios.get(endpoint, {
      responseType: 'stream',
    });

    //Piping the data into an in memory pipe.
    const passThrough = new PassThrough();
    stream.data.pipe(passThrough);

    //Uploading to our S3 bucket.
    const response = await s3
      .upload({
        Bucket: bucketName,
        Key: key,
        Body: passThrough,
      })
      .promise();

    console.log(`Sucessfully got ${response.Key}`);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (error: any) {
    console.error(`Could not download file ${error.config.url}`);
  }
};

export const handler = async () => {
  const delimiter = '/';
  const topLevelFolders = await s3
    .listObjectsV2({
      Bucket: bucketName,
      Delimiter: delimiter,
    })
    .promise();

  let date = dayjs.utc().format('YYYY-MM-DD');

  //If we already processed some files in the past.
  if (topLevelFolders.CommonPrefixes) {
    //Thanks S3 for this confusing API!
    const dates = topLevelFolders.CommonPrefixes?.map((prefix) =>
      prefix.Prefix?.replace(delimiter, '')
    );

    //S3 already sorts the folders, but not taking any chances.
    const lastProcessedDate = dates?.sort()[dates.length - 1];

    //Fetching data for the day after the last processed date.
    date = dayjs.utc(lastProcessedDate).add(1, 'day').format('YYYY-MM-DD');
  }

  console.log(`Downloading files for date ${date}`);

  //Format of the files is this: https://app.revolvair.org/storage/archive-stations/[DATE]/[DATE]_SLUG_STATION.csv
  //Example file:  https://app.revolvair.org/storage/archive-stations/2022-04-14/2022-04-14_3469958.csv

  //All the known file names. Taken from the zip file I received as an example.
  for (const filename of fileNames) {
    await migrateRevolvairToS3(`${date}/${date}_${filename}.csv`);
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'text/plain' },
    body: `Successfully downloaded ${fileNames.length} files.`,
  };
};
