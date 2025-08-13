
footer_html = """
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Source+Sans+Pro:400,600&display=swap">
<style>
html, body { margin: 0; padding: 0; background: transparent; }
.footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    color: #D3D3D3;
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 10px 20px;
    font-size: 14px;
    font-family: 'Source Sans Pro', sans-serif;
    background-color: #0e1117;  /* same as main streamlit background */
    z-index: 9999;
}
.footer a { color: #87CEEB; text-decoration: none; }
.separator { margin: 0 15px; }
.donation-address {
    background-color: #2C2C2C;
    padding: 4px 8px;
    border-radius: 5px;
    font-family: monospace;
    margin: 8px;
}
.icon-container {
    display: inline-block;
    width: 1.5em;
    text-align: center;
    cursor: pointer;
}
.copy-icon { color: #A9A9A9; transition: color 0.2s; }
.copy-icon:hover { color: #87CEEB; }
</style>

<div class="footer">
    <span>made by <a href="https://x.com/mfnatiq1" target="_blank">@mfnatiq1</a></span>
    <span class="separator">•</span>
    <span>donations:</span>
    <span id="donation-address" class="donation-address">0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7</span> 
</div>
"""
# <span class="icon-container" id="copy-btn" title="Copy to clipboard">
#         <i id="icon-copy" class="fa-solid fa-copy copy-icon"></i>
#         <i id="icon-check" class="fa-solid fa-check copy-icon" style="display:none; color:#7CFC00;"></i>
#     </span>
# <script>
# function copy_to_clipboard() {
#     var copyText = document.getElementById("donation-address").innerText;
#     var iconCopy = document.getElementById("icon-copy");
#     var iconCheck = document.getElementById("icon-check");

#     function showTick() {
#         iconCopy.style.display = 'none';
#         iconCheck.style.display = 'inline-block';
#         setTimeout(function() {
#             iconCheck.style.display = 'none';
#             iconCopy.style.display = 'inline-block';
#         }, 1500);
#     }

#     if (navigator.clipboard && navigator.clipboard.writeText) {
#         navigator.clipboard.writeText(copyText).then(showTick).catch(function() {
#             fallbackCopy();
#         });
#     } else {
#         fallbackCopy();
#     }

#     function fallbackCopy() {
#         var ta = document.createElement('textarea');
#         ta.value = copyText;
#         ta.style.position = 'fixed';
#         ta.style.left = '-9999px';
#         document.body.appendChild(ta);
#         ta.select();
#         try {
#             document.execCommand('copy');
#             showTick();
#         } catch (e) {
#             alert('Copy failed');
#         }
#         document.body.removeChild(ta);
#     }
# }

# // Immediately bind event listener when this script runs
# var copyBtn = document.getElementById('copy-btn');
# if (copyBtn) {
#     copyBtn.addEventListener('click', copy_to_clipboard);
# }
# </script>
# TODO copy icon change doesn't work, commented out for now