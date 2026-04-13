async function run(){
    const name = document.getElementById("name").value;
    const city = document.getElementById("city").value;

    const res = await fetch("http://127.0.0.1:5000/scrape",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({name, city})
    });

    const data = await res.json();

    document.getElementById("result").innerText =
        JSON.stringify(data, null, 2);
}
