package fr.tague.winfs;

import org.apache.hadoop.fs.LocalFileSystem;

/** Équivalent de LocalFileSystem (avec sommes de contrôle) basé sur NoPermRawFileSystem. */
public class NoPermLocalFileSystem extends LocalFileSystem {
    public NoPermLocalFileSystem() { super(new NoPermRawFileSystem()); }
}
