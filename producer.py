import sys
import time
import cv2
from kafka import KafkaProducer

topic = "distributed-video1"

def publish_video(video_file):
    """
    On publi depui un fichier video
    
    le parametre video_file correspond on path of the file
    """
    # demarage du producteur
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # ouverture du fichier
    video = cv2.VideoCapture(video_file)
    
    print('publication video...')

    while(video.isOpened()):
        success, frame = video.read()

        # on check que la video a bien chargé
        if not success:
            print("la video est trop lourde, ca passe mal!")
            break
        
        # on converti la vidéo en image
        ret, buffer = cv2.imencode('.jpg', frame)

        # on envoie à kafka
        producer.send(topic, buffer.tobytes())

        time.sleep(0.2)
    video.release()
    print('c est bon')

    
def publish_camera():
    """
    Video avec webcam.
    """

    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    
    camera = cv2.VideoCapture(0)
    try:
        while(True):
            success, frame = camera.read()
        
            ret, buffer = cv2.imencode('.jpg', frame)
            producer.send(topic, buffer.tobytes())
            
            # Choppier stream, reduced load on processor
            time.sleep(0.2)

    except:
        print("\nExiting.")
        sys.exit(1)

    
    camera.release()


if __name__ == '__main__':
    """
    On publie la video avec le path si donné sinon on utilise la webcam.
    """
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("publishing feed!")
        publish_camera()