var parquet = require("parquetjs-lite");
var csv = require("csv-parser");
var fs = require("fs");
var path = require("path");
const chalk = require("chalk");
var moment = require("moment");

const monthsToAdd = 6;

const readline = require("readline").createInterface({
  input: process.stdin,
  output: process.stdout,
});

let csvIndex = [];
let csvIndexCounter = 0;

var scheme = new parquet.ParquetSchema({
  c: { type: "UTF8" },
  ua: { type: "INT64" },
  b: { type: "INT64" },
  device_environment: { type: "INT64" },
  reason: { type: "INT64" },
  search_id: { type: "INT64" },
  channel: { type: "UTF8" },
  url: { type: "UTF8", optional: true },
  utm_source: { type: "UTF8" },
  utm_medium: { type: "UTF8" },
  utm_campaign: { type: "UTF8" },
  utm_term: { type: "UTF8" },
  utm_content: { type: "UTF8" },
  page_type: { type: "UTF8" },
  timestamp: { type: "UTF8" },
  imp: { type: "INT64" },
  num_distinct_users_hll_sketch: { type: "UTF8", optional: true },
  num_distinct_fb_client_id_hll_sketch: { type: "UTF8", optional: true },
  num_distinct_domain_user_id_hll_sketch: { type: "UTF8", optional: true },
  num_distinct_user_id_hll_sketch: { type: "UTF8", optional: true },
  referrer: { type: "UTF8" },
});

const getCsv = async (filePath) => {
  console.log(chalk.blue.bold(`Parsing ${path.basename(filePath)} file..`));

  return new Promise((resolve, reject) => {
    let results = [];

    fs.createReadStream(filePath)
      .pipe(csv(["url", "referrer", "utm_campaign", "utm_term"]))
      .on("data", (data) => {
        results.push(data);
      })
      .on("end", () => {
        console.log(chalk.blue.bold(`Parsing Completed.`));
        resolve(results);
      });
  });
};

const main = async (filePath, option) => {

  let reader = await parquet.ParquetReader.openFile(filePath);

  if(option == 1){
    const pathArr = filePath.split("/");
    const folderName = pathArr.find((p) => p.includes("date"));
    const fileName = pathArr.find((p) => p.includes("_0"));
    const convertedFolderName = convertFolderName(folderName);
    const convertedFileName = convertFileName(fileName);
  
    filePath = filePath
      .replace(folderName, convertedFolderName)
      .replace(fileName, convertedFileName);
  }


  try {
    let writer = await parquet.ParquetWriter.openFile(scheme, `_${filePath}`);

    // create a new cursor
    let cursor = reader.getCursor();

    // read all records from the file and print them
    let record = null;

    while ((record = await cursor.next())) {
      Object.keys(record).forEach((key) => {
        if (option == 1) {
          if (record.hasOwnProperty("timestamp") && key == "timestamp") {
            record[key] = moment(record.timestamp)
              .add(182, "days")
              .format("YYYY-MM-DD HH:mm:ss.SSS")
              .toString();
          }
        } else if (option == 2) {
          if (csvIndex[csvIndexCounter].hasOwnProperty(key)) {
            
            record[key] = csvIndex[csvIndexCounter][key];
            // Testing:
            // console.log(`CSV Index is: ${csvIndexCounter}`);
            // console.log(
            //   `${filePath} - Change ${record[key]} to ${csvIndex[csvIndexCounter][key]}`
            // );
          }
        }
      });

      // if csv list is ended then reset index to 0.
      csvIndexCounter =
      csvIndexCounter >= csvIndex.length - 1 ? 0 : csvIndexCounter + 1;
      await writer.appendRow(record);
    }

    await reader.close();
    await writer.close();
  } catch (error) {
    console.log(chalk.red.bold(error), chalk.red(filePath));
  }
};

const makeDir = (dirPath, newDirName) => {
  fs.mkdir(path.join(dirPath, newDirName), { recursive: true }, (err) => {
    if (err) {
      return console.error(err);
    }
  });
};

const convertFileName = (fileName) => {
  if (fileName.length == 17) {
    return moment(fileName, "YYYYMMDDTHHmmssfff")
      .add(182, "days")
      .format("YYYYMMDDTHHmmss_0")
      .toString();
  } else if (fileName.length == 64) {
    const date = fileName.substring(0, 8);
    const formattedDate = moment(date)
      .add(182, "days")
      .format("YYYYMMDD")
      .toString();

    return fileName.replace(date, formattedDate);
  } else return fileName;
};

const convertFolderName = (folderName) => {
  return `date=${moment(folderName.split("=")[1])
    .add(182, "days")
    .format("YYYY-MM-DD")
    .toString()}`;
};

const traverseDir = (dir, option) => {

  fs.readdirSync(dir).forEach((file) => {
    let fullPath = path.join(dir, file);

    if (fs.lstatSync(fullPath).isDirectory()) {
      const convertedFolderName = option === 1 ? convertFolderName(file) : file;
      const newTag = dir.replace("root", "_root");
      makeDir(newTag, convertedFolderName);
      traverseDir(fullPath, option);
    } else {
      var isHidden = /^\./.test(file);
      var isTempFile = file.includes("restored");
      if (!isHidden && !isTempFile) {
        main(fullPath, option);
      }
    }
  });
};

const convertParquet = async (option) => {
  const csvFile = "./csv/386 index copy.csv";
  csvIndex = await getCsv(csvFile);
  
  makeDir(__dirname, "_root");
  traverseDir("./root", option);
};

const shiftDates = (option) => {
  makeDir(__dirname, "_root");
  traverseDir("./root", option);
};

(async () => {
  /* 1. Copy the root folder inside "./root".
  /* 2. Copy csv index file inside "'./csv/"
  /* 3. Rename  csv file here at: const csvFile = "./csv/[csv-file-name].csv
  /* 4. Run node index.js 
  /* the output will be inside _root folder */

  readline.question(`1-shifting dates\n2-converting-parquet\n`, (opt) => {

    if (opt == 2) convertParquet(opt);
    else if (opt == 1) shiftDates(opt);

    readline.close();
  });
})();
