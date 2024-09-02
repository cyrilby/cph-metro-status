On the status of the chromedriver package for Linux

Previously, `chromedriver` on Linux was manually downloaded & maintained (due to needing a special 64bit ARM version), however, that breaks things when the Chromium browser is updated. Therefore, the path in the script was updated on 2 September 2024 to reference the system-wide version of `chromedriver`, which is always automatically updated via the package manager.
