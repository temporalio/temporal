module.exports = ({ context, core }) => {
  const ref = context.ref.replace('refs/heads/', '').replace('refs/tags/', '');

  // Prefix with "branch-" for branch builds
  const branchName = `branch-${ref}`;

  // Sanitize branch name to create safe Docker tag
  // Replace any non-alphanumeric (except .-_) with dash
  let safeTag = branchName.replace(/[^a-zA-Z0-9._-]/g, '-');

  // Docker tags must be lowercase and start with alphanumeric
  safeTag = safeTag.toLowerCase();
  safeTag = safeTag.replace(/^[^a-z0-9]+/, '');

  // Truncate to 128 characters (Docker tag limit)
  safeTag = safeTag.substring(0, 128);

  if (!safeTag) {
    throw new Error('Failed to generate valid Docker tag from branch name');
  }

  console.log(`Original: ${ref}`);
  console.log(`Sanitized: ${safeTag}`);

  core.setOutput('tag', safeTag);
};
