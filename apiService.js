const axios = require('axios');
require('dotenv').config();

const sendEmployeeData = async (payload) => {
    console.log(payload);

    try {
        const response = await axios.post(process.env.API_URL, payload, {
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': process.env.API_KEY,
                'x-source-system': 'samrt_prd'
            },
            timeout: 10000
        });

        const statusLabel = response.data.status || response.data.action || 'UNKNOWN';
        console.log(`API Response: ${statusLabel} (ID: ${payload.codempid})`);
        return response.data;
    } catch (error) {
        if (error.response) {
            console.error(`API Error (${error.response.status}):`, error.response.data.error);
        } else {
            console.error('Connection Error:', error.message);
        }
        throw error;
    }
};

module.exports = { sendEmployeeData };
