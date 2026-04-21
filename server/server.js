require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });

const app = require("./app");
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Servidor listo en http://localhost:${PORT}`);
});
