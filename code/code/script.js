
const timerDisplay = document.getElementById('timer');
let timeLeft = 300; // 5 minutes in seconds

function updateTimer() {
    let minutes = Math.floor(timeLeft / 60);
    let seconds = timeLeft % 60;

    minutes = minutes < 10 ? "0" + minutes : minutes;
    seconds = seconds < 10 ? "0" + seconds : seconds;

    timerDisplay.textContent = `${minutes}:${seconds}`;

    if (timeLeft <= 0) {
        clearInterval(timerInterval);
        timerDisplay.textContent = "Time's up!";
    } else {
        timeLeft--;
    }
}

updateTimer(); // Initial call to display the timer immediately
const timerInterval = setInterval(updateTimer, 1000);
