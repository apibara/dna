final: prev:
{
  # rusty_v8 downloads a prebuilt v8 from github, so we need to prefetch it
  # and pass it to the builder.
  librusty_v8 = prev.callPackage ./packages/librusty_v8.nix { };
}

