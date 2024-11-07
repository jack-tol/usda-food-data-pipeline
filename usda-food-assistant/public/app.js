document.addEventListener('DOMContentLoaded', function () {
  // Select the target node (the element you want to observe for changes)
  const targetNode = document.body; // Or use any specific element you want to observe

  // Create a new MutationObserver instance
  const observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      // Look for all instances of the Copy button
      const copyButtons = document.querySelectorAll(
        '.MuiStack-root .MuiButtonBase-root[aria-label="Copy"]'
      );

      copyButtons.forEach(function (copyButton) {
        // Hide each Copy button found
        if (copyButton) {
          copyButton.style.display = 'none';
        }
      });
    });
  });

  // Define the configuration for the observer (what types of mutations to observe)
  const config = { childList: true, subtree: true };

  // Start observing the target node for configured mutations
  observer.observe(targetNode, config);
});
