import {
  S3Client,
  SelectObjectContentCommand,
  SelectObjectContentCommandInput,
  SelectObjectContentEventStream,
} from "@aws-sdk/client-s3";
const REGION = "us-east-1";
const s3 = new S3Client({ region: REGION });

const params: SelectObjectContentCommandInput = {
  Bucket: "cardpay-staging-reward-programs",
  Key: "rewardProgramID=0x0885ce31D73b63b0Fcb1158bf37eCeaD8Ff0fC72/paymentCycle=27721626/results.parquet",
  ExpressionType: "SQL",
  Expression: "SELECT * FROM S3Object",
  InputSerialization: {
    Parquet: {},
  },
  OutputSerialization: {
    JSON: {},
  },
};

const run = async () => {
  let records: string[] = [];
  try {
    const command = new SelectObjectContentCommand(params);
    const s3Data = await s3.send(command);

    const metadata = s3Data.$metadata;
    console.log(metadata);
    const events: AsyncIterable<SelectObjectContentEventStream> =
      s3Data.Payload;
    for await (const event of events) {
      try {
        if (event?.Records) {
          if (event?.Records?.Payload) {
            const record = JSON.parse(
              Buffer.from(event.Records.Payload).toString("utf8")
            );
            // console.log(a);
            // const record = decodeURIComponent(
            //   event.Records.Payload.toString().replace(/\+|\t/g, " ")
            // );
            records.push(record);
          } else {
            console.log("skipped event, payload: ", event?.Records?.Payload);
          }
        } else if (event.Stats) {
          console.log(`Processed ${event.Stats.Details.BytesProcessed} bytes`);
        } else if (event.End) {
          console.log("SelectObjectContent completed");
        }
      } catch (err) {
        if (err instanceof TypeError) {
          console.log("error in events: ", err);
          throw err;
        }
      }
    }
  } catch (err) {
    console.log("error fetching data: ", err);
    throw err;
  }
  console.log("final records: ", records);
  return records;
};

(async () => {
  run();
})();
