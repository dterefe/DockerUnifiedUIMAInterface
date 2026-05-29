package org.texttechnologylab.DockerUnifiedUIMAInterface.exception;

/**
 * Signals that pulling a Docker image from a registry failed.
 */
public class ImagePullException extends Exception {
    private final String imageTag;

    public ImagePullException(String imageTag, String message) {
        super(message);
        this.imageTag = imageTag;
    }

    public ImagePullException(String imageTag, String message, Throwable cause) {
        super(message, cause);
        this.imageTag = imageTag;
    }

    public String getImageTag() {
        return imageTag;
    }
}
