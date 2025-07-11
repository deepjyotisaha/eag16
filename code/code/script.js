/* script.js */

// Smooth scrolling effect
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();

        document.querySelector(this.getAttribute('href')).scrollIntoView({
            behavior: 'smooth'
        });
    });
});

// Basic image gallery functionality (example)
const images = document.querySelectorAll('.gallery img');
if (images.length > 0) {
    images.forEach(img => {
        img.addEventListener('click', function () {
            // Add your image gallery logic here (e.g., modal popup)
            alert('Image clicked!'); // Placeholder
        });
    });
}