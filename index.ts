import { ParquetWriter, ParquetSchema } from "parquetjs-lite";

async function generateParquet(): Promise<void> {
  const compression = "GZIP"; // 'GZIP' or 'SNAPPY' or 'UNCOMPRESSED'
  // Define the Parquet schema
  const tsType = "TIMESTAMP_MICROS"; // 'INT64' or 'TIMESTAMP_MILLIS' or 'TIMESTAMP_MICROS'
  const schema = new ParquetSchema({
    ts: { type: tsType, compression },
    temp: { type: "DOUBLE", compression },
  });

  const fileName = `temps_${compression.toLowerCase()}.parquet`;
  const writer = await ParquetWriter.openFile(schema, fileName);

  const now = new Date();
  now.setMonth(now.getMonth() - 1);
  const startTime = now.getTime();

  const numRows = 1_000_000;
  let ts = startTime;

  const t0 = performance.now();
  let temp = 20 + Math.random() * (55 - 20);
  for (let i = 0; i < numRows; i++) {
    // Modify the temperature with a small random variation
    temp += (Math.random() - 0.5) * 0.15;
    // Ensure the temperature remains within the range [20, 55]
    temp = Math.max(20, Math.min(55, temp));
    await writer.appendRow({ ts, temp });
    if (i > 0 && i % 100_000 === 0) {
      console.log(`${((i / numRows) * 100).toFixed(2)}% rows written`);
    }
    ts += 1_000;
  }
  const t1 = performance.now();
  console.log(`Time to generate ${numRows} rows: ${t1 - t0} ms`);
  console.log(`Rows per second: ${(numRows / ((t1 - t0) / 1000)).toFixed(2)}`);
  await writer.close();
  console.log(`Parquet file written to ${fileName}`);
}

generateParquet().catch((err) => {
  console.error("Error generating Parquet file:", err);
});
