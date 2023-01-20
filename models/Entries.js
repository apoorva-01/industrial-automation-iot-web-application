import mongoose from 'mongoose';

const entriesSchema = new mongoose.Schema(
  {
    deviceName: { type: String, required: true },
    devEUI: { type: String, required: true },
    temprature: { type: String, required: true },
    humidity: { type: String, required: true },
    timestamp: { type: Date, required: true },
  }
);

const Entries = mongoose.models.Entries || mongoose.model('Entries', entriesSchema,'entries');
export default Entries;
