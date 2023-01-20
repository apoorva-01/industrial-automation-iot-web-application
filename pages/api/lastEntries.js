import nc from 'next-connect';
import Entries from '../../models/Entries';
import db from '../../utils/db';

const handler = nc();


handler.get(async (req, res) => {
    await db.connect();
    const lastEntry79f9 = await Entries.find({ devEUI: 'a8404151518379f9' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    await db.connect();
    const lastEntry79fd = await Entries.find({ devEUI: 'a8404181e18379fd' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    await db.connect();
    const lastEntry7a0e = await Entries.find({ devEUI: 'a8404152a1837a0e' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    await db.connect();
    const lastEntry7a01 = await Entries.find({ devEUI: 'a840417eb1837a01' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    await db.connect();
    const lastEntry79fe = await Entries.find({ devEUI: 'a84041c2718379fe' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    await db.connect();
    const lastEntry7a0a = await Entries.find({ devEUI: 'a84041b931837a0a' }).sort({ _id: -1 }).limit(1)
    await db.disconnect();  
    
    console.log('Latest Entries Fetched')
    res.send({
        lastEntry79f9: lastEntry79f9,
        lastEntry79fd: lastEntry79fd,
        lastEntry7a0e: lastEntry7a0e,
        lastEntry7a01: lastEntry7a01,
        lastEntry79fe: lastEntry79fe,
        lastEntry7a0a: lastEntry7a0a,
    });

});

export default handler;
