
const minutesEl = document.getElementById('minutes');
const secondsEl = document.getElementById('seconds');
let totalSeconds = 300; // 5 minutes

function updateCountdown() {
    let minutes = Math.floor(totalSeconds / 60);
    let seconds = totalSeconds % 60;

    minutes = minutes < 10 ? '0' + minutes : minutes;
    seconds = seconds < 10 ? '0' + seconds : seconds;

    minutesEl.innerText = minutes;
    secondsEl.innerText = seconds;

    totalSeconds--;

    if (totalSeconds < 0) {
        totalSeconds = 0;
    }
}

updateCountdown();
setInterval(updateCountdown, 1000);
