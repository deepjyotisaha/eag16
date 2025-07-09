
// Set the initial time in minutes
let initialMinutes = 2;
let time = initialMinutes * 60;

const countdownEl = document.getElementById('countdown');

setInterval(updateCountdown, 1000);

function updateCountdown() {
    const minutes = Math.floor(time / 60);
    let seconds = time % 60;

    seconds = seconds < 10 ? '0' + seconds : seconds;

    countdownEl.innerHTML = `${minutes}:${seconds}`;
    time--;

    if (time < 0) {
        clearInterval(); // Stop the interval when time reaches 0
        countdownEl.innerHTML = 'Time Expired';
    }
}

