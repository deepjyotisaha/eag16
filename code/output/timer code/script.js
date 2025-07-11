
const timerDisplay = document.getElementById('timer');
const messageDisplay = document.getElementById('message');
let timeLeft = 300; // 5 minutes in seconds

function updateTimer() {
    let minutes = Math.floor(timeLeft / 60);
    let seconds = timeLeft % 60;

    minutes = minutes < 10 ? '0' + minutes : minutes;
    seconds = seconds < 10 ? '0' + seconds : seconds;

    timerDisplay.textContent = minutes + ':' + seconds + ':000';

    if (timeLeft <= 0) {
        clearInterval(timerInterval);
        messageDisplay.textContent = "Time's up!";
        timerDisplay.textContent = "00:00:000";
    } else {
        timeLeft--;
    }
}

const timerInterval = setInterval(updateTimer, 1000);

