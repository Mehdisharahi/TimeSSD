import express, { Request, Response } from 'express';

const app = express();

app.get('/', (req: Request, res: Response) => {
  res.send('بات TimeSSD زنده‌ست و ۲۴/۷ کار می‌کنه! 🚀');
});

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;

app.listen(PORT, () => {
  console.log(`وب‌سرور keep-alive روی پورت ${PORT} فعال شد`);
});
