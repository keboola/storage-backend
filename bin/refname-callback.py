def convertRef(refname):
    from packaging import version

    last_tag_name = b'${LAST_TAG_IN_SINGLEREPO}'
    tag_prefix = b'${TAG_PREFIX}'

    last_tag_name = b'2.7.0'
    tag_prefix = b'prefix/'

## The below code should in inserted into split-repo.sh --refname-callback (skip the first few lines)
## ------- cut here ✂ --------
## begin copy-paste
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
## end copy-paste
## ------- cut here ✂ --------

## TESTS ##

test_cases = {
    "Test 1": {
        "input": b'refs/tags/2.7.1',
        "expected_output": b'refs/tags/SKIP-no-prefix-refs/tags/2.7.1'
    },
    "Test 2": {
        "input": b'refs/tags/2.7.1-dev',
        "expected_output": b'refs/tags/SKIP-no-prefix-refs/tags/2.7.1-dev'
    },
    "Test 3": {
        "input": b'refs/tags/CT-12345',
        "expected_output": b'refs/tags/SKIP-invalid-refs/tags/CT-12345'
    },
    "Test 4": {
        "input": b'refs/tags/debug-new-repo',
        "expected_output": b'refs/tags/SKIP-invalid-refs/tags/debug-new-repo'
    },
    "Test 5": {
        "input": b'refs/tags/prefix/debug-new-repo',
        "expected_output": b'refs/tags/SKIP-invalid-refs/tags/prefix/debug-new-repo'
    },
    "Test 6": {
        "input": b'refs/tags/prefix/2.7.1-dev',
        "expected_output": b'refs/tags/2.7.1-dev'
    },
    "Test 7": {
        "input": b'refs/tags/prefix/2.7.1',
        "expected_output": b'refs/tags/2.7.1'
    },
    "Test 8": {
        "input": b'refs/tags/all/2.7.1-dev',
        "expected_output": b'refs/tags/SKIP-no-prefix-refs/tags/all/2.7.1-dev'
    }
}

for test_name, test_data in test_cases.items():

    bold = "\x1b[1m"  # ANSI escape code for bold text
    reset_color = "\x1b[0m"  # Reset text attributes

    print(f'{bold}## {test_name}{reset_color}')

    input_value = test_data["input"]
    expected_output = test_data["expected_output"]
    result = convertRef(input_value)

    input_str = input_value.decode('utf-8')
    result_str = result.decode('utf-8')
    expected_str = expected_output.decode('utf-8')

    match = result == expected_output
    color = "\x1b[32m" if match else "\x1b[31m"  # Green for match, red for mismatch
    reset_color = "\x1b[0m"


    print(f'Input: {input_str}')
    print(f'Output: {color}{result_str}{reset_color}')
    print(f'Expected: {expected_str}')
    print('Match:', color + ('Match' if match else 'Mismatch') + reset_color)
    print()
