
function countdown() {
    var now = new Date().getTime();
    var targetDate = new Date('2024-12-31T23:59:59').getTime();
    var timeLeft = targetDate - now;

    var days = Math.floor(timeLeft / (1000 * 60 * 60 * 24));
    var hours = Math.floor((timeLeft % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    var minutes = Math.floor((timeLeft % (1000 * 60 * 60)) / (1000 * 60));
    var seconds = Math.floor((timeLeft % (1000 * 60)) / 1000);

    document.getElementById('timer').innerHTML = days + 'd ' + hours + 'h '
    + minutes + 'm ' + seconds + 's ';

    if (timeLeft < 0) {
        document.getElementById('timer').innerHTML = 'EXPIRED';
    }
}

setInterval(countdown, 1000);
