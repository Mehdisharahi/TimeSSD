import express from 'express';

const app = express();

app.get('/', (req, res) => {
  res.send('بات TimeSSD زنده‌ست و ۲۴/۷ کار می‌کنه! 🚀');
});

app.listen(8080, () => {
  console.log('وب‌سرور keep-alive روی پورت 8080 فعال شد');
});
