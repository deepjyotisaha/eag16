
// Image Carousel for Overview Section
let currentImage = 0;
const images = document.querySelectorAll('#overview img');
const totalImages = images.length;

function nextImage() {
    images[currentImage].classList.remove('active');
    currentImage = (currentImage + 1) % totalImages;
    images[currentImage].classList.add('active');
}

setInterval(nextImage, 3000); // Change image every 3 seconds

// Smooth Scrolling for Section Navigation
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();

        document.querySelector(this.getAttribute('href')).scrollIntoView({
            behavior: 'smooth'
        });
    });
});

// Initialize first image as active
if (images.length > 0) {
    images[0].classList.add('active');
}

