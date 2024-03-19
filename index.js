require('dotenv').config();
const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

// PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Middleware for parsing CSV files
const upload = multer({ dest: process.env.CSV_FILE_PATH });

// Define route for uploading CSV file
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const results = [];

    // Parse the CSV file
    fs.createReadStream(req.file.path)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        // Insert data into PostgreSQL
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await Promise.all(results.map(async (row) => {
            const { 'name.firstName': firstName, 'name.lastName': lastName, age, ...rest } = row;

            // Convert complex properties to nested structure
            const address = {
              line1: rest['address.line1'],
              line2: rest['address.line2'],
              city: rest['address.city'],
              state: rest['address.state'],
            };

            // Insert data into PostgreSQL table
            await client.query(
              'INSERT INTO public.users (first_name, last_name, age, address) VALUES ($1, $2, $3, $4)',
              [firstName, lastName, age, address]
            );
          }));
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        } finally {
          client.release();
          // Remove uploaded file
          fs.unlinkSync(req.file.path);
        }

        res.status(200).json({ message: 'Data uploaded successfully' });
      });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

