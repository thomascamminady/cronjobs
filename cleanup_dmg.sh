#!/bin/bash
# cleanup_dmg.sh - Delete .dmg files older than 15 minutes from Downloads

DOWNLOADS="$HOME/Downloads"

find "$DOWNLOADS" -type f -name "*.dmg" -mmin +15 -delete

