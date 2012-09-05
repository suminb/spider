

def make_absolute_url(base_url, relative_url):
    """
    Converts a relative URL to an absolute (fully qualified) URL.

    This is a (heavily) modified function taken from
    http://stackoverflow.com/questions/589833/how-to-find-a-relative-url-and-translate-it-to-an-absolute-url-in-python
    """

    from urlparse import urlparse, urljoin

    url = urlparse(relative_url)

    # if relative to domain
    if url.scheme == url.netloc == "":
        return urljoin(base_url, relative_url)

    else:
        return relative_url

if __name__ == "__main__":
    # simple use cases
    print make_absolute_url("http://test.com/foo/bar/", "../adsf/qwer/zxcv")