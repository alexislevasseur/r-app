import {
  App,
  StackProps,
  Cron,
  Function,
  Stack,
  Bucket,
} from '@serverless-stack/resources';

export default class MyStack extends Stack {
  constructor(scope: App, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucket = new Bucket(this, 'airQualityBucket');

    const downloaderFn = new Function(this, 'DownloaderFn', {
      memorySize: 512,
      handler: 'src/downloader.handler',
      environment: { s3BucketName: bucket.bucketName },
      permissions: [bucket],
    });

    if (!process.env.IS_LOCAL) {
      new Cron(this, 'downloaderFnCron', {
        schedule: 'cron(0 9 * * ? *)',
        job: downloaderFn,
      });
    }

    // Show the endpoint in the output
    this.addOutputs({
      S3BucketName: bucket.bucketName,
    });
  }
}
