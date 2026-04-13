const API_URL = "https://syndicpro-backend-e8h4.onrender.com";

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
        📞 ${data.phone} (${data.phone_conf}%)
        <br>
        📧 ${data.email} (${data.email_conf}%)
        <br>
        📊 Score global: ${data.global_conf}%
    `;
}
