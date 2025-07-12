
function sortTable(table, col, ascending = false) {
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.querySelectorAll("tr"));

    const sortedRows = rows.sort((a, b) => {
        const aVal = a.querySelectorAll("td")[col].textContent.trim();
        const bVal = b.querySelectorAll("td")[col].textContent.trim();

        if (ascending) {
            return aVal.localeCompare(bVal);
        } else {
            return bVal.localeCompare(aVal);
        }
    });

    // Remove existing rows
    while (tbody.firstChild) {
        tbody.removeChild(tbody.firstChild);
    }

    // Add sorted rows
    tbody.append(...sortedRows);

    // Toggle ascending/descending
    return !ascending;
}


document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("table").forEach(table => {
        const headers = table.querySelectorAll("th");
        let ascending = false;

        headers.forEach((header, col) => {
            header.addEventListener("click", () => {
                ascending = sortTable(table, col, ascending);
            });
        });
    });
});
