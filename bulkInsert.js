const oracledb = require('oracledb');
const fs = require('fs');
const csv = require('csv-parser');

// Configuration for Oracle database
const dbConfig = {
  user: 'payments',
  password: 'payments123',
  connectString: '10.150.26.144:1521/Soundbox'  // e.g., 'localhost:1521/ORCL'
};

// Path to the CSV file
const csvFilePath = 'test.csv';  // Update this with your CSV file path

// Function to create a connection pool
async function createPool() {
  try {
    await oracledb.createPool(dbConfig);
    console.log('Connection pool created');
  } catch (err) {
    console.error('Error creating connection pool:', err);
    process.exit(1);
  }
}

function generateRandomData() {
    return {
      amount: Math.random() * 1000,  // Random amount between 0 and 1000
      count: Math.floor(Math.random() * 100),  // Random count between 0 and 99
     
    };
  }
  
// Function to insert data from CSV into Oracle
async function insertDataFromCSV() {
  let connection;

  try {
    connection = await oracledb.getConnection();
    console.log('Database connection successful');

    // Read data from CSV
    const data = [];
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const randomData = generateRandomData();
        // Assuming CSV columns match the Oracle table columns
        // data.push([
        //   row['PID'],  // Adjust the column names as per your CSV
        //   randomData.amount,
        //   randomData.count
        // ]);
        data.push([
          row['mobile_number'],  // Adjust the column names as per your CSV
          row['uuid'],
          row['vpa']
        ]);
      })
      .on('end', async () => {
        console.log('CSV file successfully processed');

        // SQL for bulk insert
        // const insertSql = `
        //   INSERT INTO summary (uuid_vpa_assoc, amount, count)
        //   VALUES (:1, :2, :3)
        // `;
         const insertSql = `
          INSERT INTO  payments.uuid (mobile_number, uuid, vpa)
          VALUES (:1, :2, :3)
        `;

        try {
          const result = await connection.executeMany(insertSql, data, {
            autoCommit: true
          });
          console.log(`Inserted ${result.rowsAffected} rows`);
        } catch (err) {
          console.error('Error during insert:', err);
        } finally {
          await connection.close();
          console.log('Connection closed');
        }
      });
  } catch (err) {
    console.error('Error:', err);
    if (connection) {
      try {
        await connection.close();
      } catch (closeErr) {
        console.error('Error closing connection:', closeErr);
      }
    }
  }
}

// Run the script
(async () => {
  await createPool();
  await insertDataFromCSV();
})();
