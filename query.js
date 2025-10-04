// queries.js - All MongoDB queries for plp_bookstore

const { MongoClient } = require('mongodb');

// Connection URI
const uri = 'mongodb://localhost:27017'; // Replace with your Atlas URI if needed

// Database and collection names
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    console.log('\n--- Basic Queries ---');

    // 1. Find all books in Fiction genre
    const fictionBooks = await collection.find({ genre: "Fiction" }).toArray();
    console.log('Fiction Books:', fictionBooks.map(b => b.title));

    // 2. Find books published after 1950
    const post1950 = await collection.find({ published_year: { $gt: 1950 } }).toArray();
    console.log('Books published after 1950:', post1950.map(b => b.title));

    // 3. Find books by George Orwell
    const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
    console.log('Books by George Orwell:', orwellBooks.map(b => b.title));

    // 4. Update price of "1984"
    await collection.updateOne({ title: "1984" }, { $set: { price: 12.99 } });
    console.log('Updated price of "1984"');

    // 5. Delete "Moby Dick"
    await collection.deleteOne({ title: "Moby Dick" });
    console.log('Deleted "Moby Dick"');

    console.log('\n--- Advanced Queries ---');

    // 6. Find in-stock books published after 2010
    const recentInStock = await collection.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray();
    console.log('In stock & published after 2010:', recentInStock.map(b => b.title));

    // 7. Projection: title, author, price
    const projected = await collection.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray();
    console.log('Projection sample:', projected.slice(0, 3));

    // 8. Sort by price ascending
    const priceAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log('Cheapest book:', priceAsc[0].title);

    // 9. Sort by price descending
    const priceDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log('Most expensive book:', priceDesc[0].title);

    // 10. Pagination: first 5 books
    const page1 = await collection.find().skip(0).limit(5).toArray();
    console.log('Page 1 books:', page1.map(b => b.title));

    console.log('\n--- Aggregation Pipelines ---');

    // 11. Average price by genre
    const avgPriceByGenre = await collection.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray();
    console.log('Average price by genre:', avgPriceByGenre);

    // 12. Author with most books
    const topAuthor = await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('Author with most books:', topAuthor[0]);

    // 13. Books grouped by decade
    const booksByDecade = await collection.aggregate([
      { $group: { _id: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] }, count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('Books by decade:', booksByDecade);

    console.log('\n--- Indexing ---');

    // 14. Create index on title
    await collection.createIndex({ title: 1 });
    console.log('Index created on title');

    // 15. Compound index on author and published_year
    await collection.createIndex({ author: 1, published_year: -1 });
    console.log('Compound index created on author and published_year');

    // 16. Explain query performance
    const explainResult = await collection.find({ title: "1984" }).explain("executionStats");
    console.log('Explain query for title "1984":', explainResult.executionStats);

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

// Run all queries
runQueries().catch(console.error);
