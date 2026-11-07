"""login state kept in st.session_state, shared by every page.

two states: no "user_email" key (logged out) and a "user_email" key (logged in, trial started if the user was new).
a failed trial start leaves the key unset, so the next rerun tries again instead of showing a logged in user with no record.
"""
from logging import Logger
from typing import Callable, MutableMapping, Optional


def sync_login(session: MutableMapping, user, logger: Logger,
               start_trial: Optional[Callable[[str, Logger], Optional[str]]] = None) -> Optional[str]:
    """call once per page run with st.session_state and st.user. returns an error message to show, or None."""
    logged_in = bool(user) and "email" in user
    if logged_in and "user_email" not in session:
        email = user.email
        if start_trial is not None:
            err = start_trial(email, logger)
            if err:
                return err
        session["user_email"] = email
    elif not logged_in and "user_email" in session:
        del session["user_email"]
        session.pop("user_object", None)
    return None
