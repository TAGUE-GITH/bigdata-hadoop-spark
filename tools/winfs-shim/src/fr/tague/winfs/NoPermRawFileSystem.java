package fr.tague.winfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * RawLocalFileSystem utilisable sous Windows sans winutils.exe ni hadoop.dll :
 * les permissions POSIX sont ignorées et le listing passe par java.io.File.
 */
public class NoPermRawFileSystem extends RawLocalFileSystem {
    @Override public void setPermission(Path p, FsPermission permission) throws IOException { }
    @Override public void setOwner(Path p, String username, String groupname) throws IOException { }

    /** Pas de liens symboliques gérés : évite FileUtil.readLink (winutils). */
    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
        return getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File local = pathToFile(f);
        if (!local.exists()) throw new FileNotFoundException("File " + f + " does not exist");
        if (!local.isDirectory()) return new FileStatus[] { getFileStatus(f) };
        String[] noms = local.list();
        if (noms == null) throw new IOException("Impossible de lister " + f);
        List<FileStatus> resultat = new ArrayList<>(noms.length);
        for (String nom : noms) {
            try {
                resultat.add(getFileStatus(new Path(f, nom)));
            } catch (FileNotFoundException supprimeEntreTemps) {
                // fichier supprimé pendant le listing : ignoré, comme dans Hadoop
            }
        }
        return resultat.toArray(new FileStatus[0]);
    }
}
