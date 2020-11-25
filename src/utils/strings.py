import re

def normalize_text(s):
    """
    Normalize input string
    """
    if s is None or len(s) == 0:
        return ""

    # trim text
    text = s.strip()
    # remove html forgotten tags
    text = re.sub(re.compile("<.*?>"), "", text)
    # remove double space
    text = re.sub(" +", " ", text)

    return text


def normalize_keyword(s, strict=False):
    """
    Remove accents from input string
    """
    if s is None or len(s) == 0:
        return ""

    text = "".join(s)
    text = re.sub("[!@#$<>|]", "", text)
    text = normalize_text(text)

    return text
