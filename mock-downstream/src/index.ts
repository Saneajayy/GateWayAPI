import express from 'express';

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());

let state = 'healthy'; // 'healthy', 'failing', 'slow'

app.post('/admin/fault', (req, res) => {
  state = req.body.state;
  console.log(`Mock Server State changed to: ${state}`);
  res.json({ message: `State set to ${state}` });
});

// Mock downstream endpoint
app.use(async (req, res, next) => {
  if (req.path === '/admin/fault') return next();

  if (state === 'failing') {
    return res.status(500).json({ error: 'Internal Server Error' });
  }

  if (state === 'slow') {
    await new Promise(r => setTimeout(r, 2000)); // 2 second delay
  }

  // Realistic baseline latency (50ms) instead of instant
  if (state === 'healthy') {
    await new Promise(r => setTimeout(r, 50)); 
  }

  res.json({
    message: 'Hello from mock downstream',
    path: req.path,
    method: req.method,
    headers: req.headers,
    timestamp: new Date().toISOString()
  });
});

app.listen(PORT, () => {
  console.log(`Mock downstream listening on port ${PORT}`);
});
