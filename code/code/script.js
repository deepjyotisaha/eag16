
const minutesSpan = document.getElementById('minutes');
const secondsSpan = document.getElementById('seconds');
let minutes = parseInt(minutesSpan.textContent);
let seconds = parseInt(secondsSpan.textContent);

function updateTimer() {
    seconds--;
    if (seconds < 0) {
        minutes--;
        seconds = 59;
        if (minutes < 0) {
            minutes = 0;
            seconds = 0;
            clearInterval(interval);
            alert('Countdown finished!');
        }
    }

    minutesSpan.textContent = minutes.toString();
    secondsSpan.textContent = seconds < 10 ? '0' + seconds.toString() : seconds.toString();
}

const interval = setInterval(updateTimer, 1000);

// Dark mode toggle
const darkModeToggle = document.createElement('button');
darkModeToggle.classList.add('dark-mode-toggle');
darkModeToggle.textContent = 'Toggle Dark Mode';
document.body.appendChild(darkModeToggle);

darkModeToggle.addEventListener('click', () => {
    document.body.classList.toggle('dark-mode');
});
