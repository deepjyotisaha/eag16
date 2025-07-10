
const countdownElement = document.getElementById('countdown');
let timeLeft = 300;

function updateTimer() {
    let minutes = Math.floor(timeLeft / 60);
    let seconds = timeLeft % 60;

    minutes = minutes < 10 ? '0' + minutes : minutes;
    seconds = seconds < 10 ? '0' + seconds : seconds;

    countdownElement.textContent = `${minutes}:${seconds}`;
    timeLeft--;

    if (timeLeft < 0) {
        clearInterval(timerInterval);
        countdownElement.textContent = 'Time is up!';
    }
}

updateTimer();
const timerInterval = setInterval(updateTimer, 1000);
