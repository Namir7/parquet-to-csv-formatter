const process = require('process');
const pqt = require('parquetjs-lite');

const { ParquetReader } = pqt;

const { stringify } = require("csv-stringify");
const { writeFile } = require("fs");
const { Readable } = require("stream");

const COLUMNS_STRUCT = {
  id: 'id',
  user_id: 'user_id',
  active: 'active',
  balance: 'balance',
  created_at: 'created_at',
  updated_at: 'updated_at',
  debt: 'debt'
};

const writerCSV = async () => {
  const inputFilePath = process.argv[2];
  const outputFilePath = process.argv[3];

  console.log("read parquet from file", inputFilePath);

  let reader = await ParquetReader.openFile(inputFilePath);
 
  let cursor = reader.getCursor();
  
  let record = null;
  const acc = []
  
  while (record = await cursor.next()) {
    console.log(record);
    acc.push(record);
  }

  const readStream = Readable.from(acc);

  readStream.pipe(
    stringify(
      {
        header: true,
        columns: COLUMNS_STRUCT,
      },
      function (error, output) {
        writeFile(outputFilePath, output, () => {});
      }
    )
  );

  readStream.on("end", async function () {
    await reader.close();
  });
};

writerCSV();
