package io.airlift.http.client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public final class FileBodyGenerator
        implements BodyGenerator
{
    private final Path path;

    public FileBodyGenerator(Path path)
    {
        this.path = requireNonNull(path, "path is null");
    }

    public Path getPath()
    {
        return path;
    }

    public long length()
    {
        try {
            return Files.size(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
