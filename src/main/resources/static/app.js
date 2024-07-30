const evtSource = new EventSource("/events" + window.location.search)
const sesssions = {}
const canvas = document.getElementById("mainCanvas");
const ctx = canvas.getContext("2d");

const httpGet = (path, params) => {
    const queryParams = new URLSearchParams();
    for(const param in params) {
        if(params.hasOwnProperty(param)){
            queryParams .append(param, params[param]);
        }
    }
    const xhr = new XMLHttpRequest();
    xhr.open("GET", path + (queryParams.size < 1 ? '' :  '?' + queryParams.toString()));
    xhr.send();
}
const draw = ({x, y}) => {
    ctx.beginPath();
    ctx.arc(x, y, 10, 0, 2 * Math.PI);
    ctx.stroke();
}
const postUpdate = ({offsetX: x, offsetY: y}) => httpGet('/position', {x, y})

evtSource.addEventListener("session_started", ({data: session}) => {
    sesssions[session] = setInterval(() => {
        httpGet('/ping', {session})
    }, 40000);
});
evtSource.addEventListener("session_ended", ({data}) => { clearInterval( sesssions[data]) });
evtSource.addEventListener("position_changed", ({data}) => { draw(JSON.parse(atob(data))) });

