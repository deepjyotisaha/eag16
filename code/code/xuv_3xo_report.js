
document.addEventListener('DOMContentLoaded', function() {
    // Smooth scrolling for navigation links
    const navLinks = document.querySelectorAll('a[href^="#"]');
    navLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);
            if (targetElement) {
                window.scrollTo({
                    top: targetElement.offsetTop - 50, // adjust for header height
                    behavior: 'smooth'
                });
            }
        });
    });

    // Example: Adding a dynamic element
    const header = document.querySelector('header');
    const dynamicElement = document.createElement('div');
    dynamicElement.textContent = 'Updated: ' + new Date().toLocaleDateString();
    dynamicElement.style.textAlign = 'center';
    header.appendChild(dynamicElement);
});
