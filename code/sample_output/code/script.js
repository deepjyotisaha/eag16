
const timerDisplay = document.getElementById('timerDisplay');
let timeLeft = 300; // 5 minutes in seconds

function updateTimer() {
    let minutes = Math.floor(timeLeft / 60);
    let seconds = timeLeft % 60;
    seconds = seconds < 10 ? '0' + seconds : seconds;
    timerDisplay.textContent = `${minutes}:${seconds}`;
    timeLeft--;

    if (timeLeft < 0) {
        timerDisplay.textContent = "Time's up!";
        clearInterval(timerInterval);
    }
}

const timerInterval = setInterval(updateTimer, 1000);
