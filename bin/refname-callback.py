def convertRef(refname):
    from packaging import version

    last_tag_name = b'${LAST_TAG_IN_SINGLEREPO}'
    tag_prefix = b'${TAG_PREFIX}'

    last_tag_name = b'2.7.0'
    tag_prefix = b'prefix/'

## ------- cut here ✂ --------

    # print(b'Checking %s for prefix %s' % (refname, b'${TAG_PREFIX}'))
    # not a tag -> keep as is
    if not refname.startswith(b'refs/tags/'):
        return refname

    tag_name = refname.decode('utf-8').split('/')[-1]
    try:
        if version.parse(tag_name) <= version.parse(last_tag_name.decode('UTF-8')):
            # print('[%s] skipped' % (refname.decode('utf-8')))
            return b'refs/tags/SKIP-old-' + refname
    except version.InvalidVersion:
        return b'refs/tags/SKIP-invalid-' + refname

    # tag, but not matching prefix -> SKIP
    if not refname.startswith(b'refs/tags/' + tag_prefix):
        # print('[%s] skipped' % (refname.decode('utf-8')))
        return b'refs/tags/SKIP-no-prefix-' + refname

    # tag, with correct prefix -> strip prefix
    rewritten_tag = refname[len(b'refs/tags/' + tag_prefix):]

    print('[%s] rewritten to [%s]' % (refname.decode('utf-8'), rewritten_tag.decode('utf-8')))
    return b'refs/tags/' + rewritten_tag

## ------- cut here ✂ --------

## TESTS ##
print(convertRef(b'refs/tags/2.7.1'))
print(convertRef(b'refs/tags/2.7.1-dev'))
print(convertRef(b'refs/tags/CT-12345'))
print(convertRef(b'refs/tags/debug-new-repo'))
print(convertRef(b'refs/tags/prefix/debug-new-repo'))
print(convertRef(b'refs/tags/prefix/2.7.1-dev'))
print(convertRef(b'refs/tags/prefix/2.7.1'))
print(convertRef(b'refs/tags/all/2.7.1-dev'))
