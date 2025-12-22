const mysql = require('mysql2')

const envConfig = process.env.SQL_CONFIG;

let sqlConfig;
try {
  sqlConfig = envConfig ? JSON.parse(envConfig) : {
    host: process.env.MYSQLHOST,
    database: process.env.MYSQLDATABASE,
    user: process.env.MYSQLUSER,
    password: process.env.MYSQLPASSWORD,
    port: 3306
  };
} catch (error) {
  console.error('Error parsing SQL_CONFIG:', error);
  // Fallback or rethrow depending on desired behavior. 
  // For now, let's allow it to try the env vars or fail later.
  sqlConfig = {
    host: process.env.MYSQLHOST,
    database: process.env.MYSQLDATABASE,
    user: process.env.MYSQLUSER,
    password: process.env.MYSQLPASSWORD,
    port: 3306
  };
}

// Log config for debugging (hide password)
console.log('SQL Configuration:', { ...sqlConfig, password: '*****' });

const connection = mysql.createConnection(sqlConfig);

connection.connect(err => {
  if (err) {
    console.error('Error connecting to database:', err);
    // Don't throw here, let the query handle it or process exit?
    // Usually better to log and maybe exit if critical.
  } else {
    console.log('Successfully connected to the database.');
  }
});

connection.on('error', err => {
  console.error('Database connection error:', err);
  if (err.code === 'PROTOCOL_CONNECTION_LOST') {
    // Reconnect logic or exit
  }
});

exports.select = (table, clause = '', sortClause = '', skip = 0, limit = 1) =>
  query(
    `SELECT * FROM ${table} ${clause} ${sortClause} LIMIT ${skip}, ${limit}`,
    {},
    identity => identity
  )

exports.insert = (table, item) =>
  query(`INSERT INTO ${table} SET ?`, item, () => item)

exports.update = (table, item) =>
  query(
    `UPDATE ${table} SET ? WHERE _id = ${connection.escape(item._id)}`,
    item,
    () => item
  )

exports.deleteOne = (table, itemId) =>
  query(
    `DELETE FROM ${table} WHERE _id = ${connection.escape(itemId)}`,
    {},
    result => result.affectedRows
  )

exports.count = (table, clause) =>
  query(
    `SELECT COUNT(*) FROM ${table} ${clause}`,
    {},
    result => result[0]['COUNT(*)']
  )

exports.describeDatabase = () =>
  query('SHOW TABLES', {}, async result => {
    const tables = result.map(entry => entry[`Tables_in_${sqlConfig.database}`])

    return Promise.all(
      tables.map(async table => {
        const columns = await describeTable(table)

        return {
          table,
          columns
        }
      })
    )
  })

const describeTable = table =>
  query(`DESCRIBE ${table}`, {}, result => {
    return result.map(entry => {
      return {
        name: entry['Field'],
        type: entry['Type'],
        isPrimary: entry['Key'] === 'PRI'
      }
    })
  })

const query = (query, values, handler) =>
  new Promise((resolve, reject) => {
    connection.query(query, values, (err, results, fields) => {
      if (err) {
        console.log(err);
        return reject(err)
      }

      resolve(handler(results, fields))
    })
  })
