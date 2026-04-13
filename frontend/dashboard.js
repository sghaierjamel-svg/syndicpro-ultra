async function loadStats(){
    const res = await fetch("http://127.0.0.1:5000/stats");
    const data = await res.json();

    document.getElementById("stats").innerHTML = `
        Total: ${data.total}<br>
        Avg confidence: ${data.avg_confidence}%
    `;
}
