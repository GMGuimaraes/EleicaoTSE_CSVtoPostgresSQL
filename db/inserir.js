const client = require('./connection');

exports.query = async (query, values) => {
  const { rows } = await client.query(query, values);
  return rows;
};

