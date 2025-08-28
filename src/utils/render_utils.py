donation_address = "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"

footer_html = f"""
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Source+Sans+Pro:400,600&display=swap">
<style>
html, body {{ margin: 0; padding: 0; background: transparent; }}
/* Kill extra bottom padding Streamlit adds */
.block-container {{
    padding-bottom: 1rem !important;
}}
.footer {{
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
}}
.footer a {{ color: #87CEEB; text-decoration: none; }}
.separator {{ margin: 0 15px; }}
.donation-address {{
    background-color: #2C2C2C;
    padding: 4px 8px;
    border-radius: 5px;
    font-family: monospace;
    margin: 8px;
}}
.icon-container {{
    display: inline-block;
    width: 1.5em;
    text-align: center;
    cursor: pointer;
}}
.copy-icon {{ color: #A9A9A9; transition: color 0.2s; }}
.copy-icon:hover {{ color: #87CEEB; }}
</style>

<div class="footer">
    <span>
    made by <a href="https://x.com/fnatiqmambo" target="_blank">@fnatiqmambo</a>
    (formerly <a href="https://x.com/mfnatiq1" target="_blank">@mfnatiq1</a>)
    </span>
    <span class="separator">•</span>
    <span>donations:</span>
    <span id="donation-address" class="donation-address">{donation_address}</span> 
    <span class="icon-container" id="copy-btn" title="Copy to clipboard">
        <i id="icon-copy" class="fa-solid fa-copy copy-icon"></i>
        <i id="icon-check" class="fa-solid fa-check copy-icon" style="display:none; color:#7CFC00;"></i>
    </span>
</div>
"""

# JavaScript component for copy functionality
copy_script = f"""
<script>
function copy_to_clipboard() {{
    var copyText = "{donation_address}";
    var iconCopy = parent.document.getElementById("icon-copy");
    var iconCheck = parent.document.getElementById("icon-check");

    function showTick() {{
        if (iconCopy && iconCheck) {{
            iconCopy.style.display = 'none';
            iconCheck.style.display = 'inline-block';
            setTimeout(function() {{
                iconCheck.style.display = 'none';
                iconCopy.style.display = 'inline-block';
            }}, 1500);
        }}
    }}

    if (navigator.clipboard && navigator.clipboard.writeText) {{
        navigator.clipboard.writeText(copyText).then(showTick).catch(function() {{
            fallbackCopy();
        }});
    }} else {{
        fallbackCopy();
    }}

    function fallbackCopy() {{
        var ta = document.createElement('textarea');
        ta.value = copyText;
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        try {{
            document.execCommand('copy');
            showTick();
        }} catch (e) {{
            alert('Copy failed');
        }}
        document.body.removeChild(ta);
    }}
}}

// Function to attach event listener
function attachCopyEvent() {{
    var copyBtn = parent.document.getElementById('copy-btn');
    if (copyBtn && !copyBtn.hasAttribute('data-listener-attached')) {{
        copyBtn.addEventListener('click', copy_to_clipboard);
        copyBtn.setAttribute('data-listener-attached', 'true');
        return true;
    }}
    return false;
}}

// Try to attach immediately
if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', attachCopyEvent);
}} else {{
    attachCopyEvent();
}}

// Fallback for Streamlit's dynamic content loading
setTimeout(function() {{
    if (!attachCopyEvent()) {{
        var observer = new MutationObserver(function(mutations) {{
            mutations.forEach(function(mutation) {{
                if (mutation.type === 'childList') {{
                    if (attachCopyEvent()) {{
                        observer.disconnect();
                    }}
                }}
            }});
        }});
        if (parent.document.body) {{
            observer.observe(parent.document.body, {{ childList: true, subtree: true }});
            setTimeout(function() {{
                observer.disconnect();
            }}, 5000);
        }}
    }}
}}, 100);
</script>
"""