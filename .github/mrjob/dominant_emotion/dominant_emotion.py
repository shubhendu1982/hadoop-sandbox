from mrjob.job import MRJob
from mrjob.step import MRStep

class DominantEmotion(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_emotions,
                   reducer=self.reducer_count_emotions)
        ]

    def mapper_get_emotions(self, _, line):
        # Skip the header
        if line.startswith('User_ID'):
            return

        parts = line.split(',')

        # Ensure there are enough parts in the line to avoid index errors
        if len(parts) > 9:
            # Extract gender and dominant emotion
            gender = parts[2]
            emotion = parts[9]

            yield gender, emotion

    def reducer_count_emotions(self, key, values):
        emotion_counts = {}
        for emotion in values:
            if emotion in emotion_counts:
                emotion_counts[emotion] += 1
            else:
                emotion_counts[emotion] = 1
        yield key, emotion_counts

if __name__ == '__main__':
    DominantEmotion.run()
