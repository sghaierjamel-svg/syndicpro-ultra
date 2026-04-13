const API_URL = "https://syndicpro-backend.onrender.com";

async function run(){
    const name = document.getElementById("name").value;
    const city = document.getElementById("city").value;

    const res = await fetch(`${API_URL}/scrape`, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({name, city})
    });

    const data = await res.json();

    document.getElementById("result").innerHTML = `
        <h3>Résultat</h3>

        📞 ${data.phone} (${data.phone_conf}%)
        <br><br>

        📧 ${data.email} (${data.email_conf}%)
        <br><br>

        📊 Confiance globale: <b>${data.global_conf}%</b>
    `;
}
