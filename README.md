# icloud-photo-synchroniser

This is a very very simple program to help me maintain a backup of my iCloud photo library.
If you are like me, you might have enjoyed the high quality Apple Photos library, but want
to self host a mirror of the data.

You might look into how iCloud security works, see about logging in on Linux, see the very
janky solutions for this problem which mostly look like pwning yourself. You might note that
on Windows, iCloud photos are presented as as a virtual drive. You might decide that you can
rsync from WSL2 to your Linux server to store photos, using filename and size based sync to
avoid overwrites.

Then, later, you might move the Windows machine into a VM so you can have your server do all
the work of running the cronjob. You might discover that Apple photos are uniquely stored by
name, and are presented to Windows with renames (e.g. `IMG_0001.jpg`, `IMG_0001 (2).jpg`).
You might discover that how this occurs is non-deterministic, so you now have a bunch of renamed
duplicates.

You might also be using Immich on Linux so renaming external files actually messes up your
organisation, and so you need to sync only the new files while avoiding duplicating because
if you duplicate images, you duplicate them in your library.

And now you realise that rsync can no longer help you. Enter this program, which indexes all
your iCloud photos and adds thew new ones to a directory of your choice based on SHA256.

This is not my best Rust, it's a 2 hour job to solve a problem.
