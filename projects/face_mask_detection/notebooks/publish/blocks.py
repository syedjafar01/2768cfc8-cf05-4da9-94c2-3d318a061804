import razor.flow as rf
import pandas as pd
from PIL import Image
import numpy as np
import typing as t
import random



@rf.block
class DataFeeder:
    __publish__ = True
    __label__ = 'DataFeeder'

    def run(self):
        data: rf.SeriesOutput[t.Any]
            
        all_images = np.load('/home/aios/projectspace/all_images_arr.npz')['images']
        all_labels = np.load('/home/aios/projectspace/all_labels_arr.npz')['labels']
        
        all_labels = all_labels.tolist()
        
        for i in range(len(df)):
            image = all_images[i]
            
            image = image / 255.0
            label = [0, 1] if all_labels[i] == 1 else [1, 0]

            self.data.put({"images": image, "labels": np.array(label)})
            
            
            
# @rf.block
# class GivePose:
#     __publish__ = True
#     __label__ = 'GivePose'
    
#     img_name : str
#     landmarks_and_image : rf.SeriesOutput[t.Any]
    
#     def run(self):
        
#         self.logger.debug('Running GivePose')
        
#         for name in self.img_name:
            
#             detector = model_zoo.get_model('faster_rcnn_fpn_resnet50_v1b_coco', pretrained=True, root = psp('model_zoo'))
#             pose_net = model_zoo.get_model('simple_pose_resnet50_v1d', pretrained=True, root = psp('model_zoo'))

#             detector.reset_class(["person"], reuse_weights=['person'])        

#             ##
#             x, img = gluon_data.transforms.presets.ssd.load_test(f'/root/projectspace/infer_on_this/{name}', short=512)

#             class_IDs, scores, bounding_boxs = detector(x)
#             pose_input, upscale_bbox = detector_to_simple_pose(img, class_IDs, scores, bounding_boxs)

#             predicted_heatmap = pose_net(pose_input)
#             pred_coords, confidence = heatmap_to_coord(predicted_heatmap, upscale_bbox)

#             pred_coords = pred_coords.asnumpy()
#             confidence = confidence.asnumpy()
#             ##
#             self.landmarks_and_image.put((pred_coords,img))


#         self.logger.debug('Completed GivePose')
        
        
        

        
# @rf.block
# class ExtractFaceRegion:
#     __publish__ = True
#     __label__ = 'ExtractFaces'
    
#     landmarks_and_image : rf.SeriesInput[t.Any]
#     xtracted_faces : rf.SeriesOutput[t.Any]
#     face_cords_and_image : rf.SeriesOutput[t.Any]
    
#     def run(self):
     
#         self.logger.debug('Running ExtractFaceRegion')
        
#         for (landmarks, image) in self.landmarks_and_image:
            
#             face_list = []
#             h,w,c = image.shape
#             orig_face_cords = pd.DataFrame(columns=['y1','y2','x1','x2'],index=[i for i in range(len(landmarks))])

#             xtracted_faces = []
#             for j in range(len(landmarks)):
#                 landmark = landmarks[j]

#                 n = landmark[0]
#                 lear = landmark[3]
#                 rear = landmark[4]

#                 n_lear = np.absolute(n[0]-lear[0]) #np.linalg.norm(n-lear)
#                 n_rear = np.absolute(n[0]-rear[0]) #np.linalg.norm(n-rear)

#                 d = max([n_lear, n_rear])
#                 d = int(d)

#                 x1 = int(max([0, n[0] - d]))
#                 x2 = int(min([n[0] + d, w]))
#                 y1 = int(max([0, n[1] - d]))
#                 y2 = int(min([n[1] + d, h]))

#                 face = image[y1:y2,x1:x2]
#                 if min(face.shape[:2]) == 0:
#                     continue
#                 else:

#                     orig_face_cords.iloc[j] = [y1,y2,x1,x2]
#                     self.xtracted_faces.put(face)

#             self.face_cords_and_image.put((orig_face_cords,image))


#         self.logger.debug('Completed ExtractFaceRegion')
        
        
        
        
# @rf.block
# class LoadFaces:
#     __publish__ = True
#     __label__ = 'LoadFaces'
    
#     faces : rf.SeriesInput[t.Any]
#     data : rf.SeriesOutput[t.Any]
    
#     def run(self):
        
#         self.logger.debug('Running LoadFaces')
        
#         for face in self.faces:

#             image = Image.fromarray(face, 'RGB')
#             image = image.resize((100,100))
#             image = np.asarray(image)
#             image = image / 255.0
            
#             self.data.put({"images":image})
        
#         self.logger.debug('Completed LoadFaces')
        
        
        
# @rf.block
# class SavePredsMask:
#     __publish__ = True
#     __label__ = 'SavePredsMask'
    
#     face_cords_and_image : rf.SeriesInput[t.Any]
#     model_output : rf.SeriesInput[t.Any]

#     def run(self):

#         self.logger.debug('Running SavePredsMask')
        
#         for cords, test_image in self.face_cords_and_image: 
#             preds = []

#             for batch_output in self.model_output:
#                 preds.extend(np.argmax(batch_output['dense3'], axis=1))

#             ##plot

#             fig, axes = plt.subplots(1,1, figsize = (10,8))
#             axes.imshow(test_image)

#             len_faces = len(cords)

#             for num in range(len_faces):
#                 y1,y2,x1,x2 = cords.iloc[num].values
#                 color = 'red' if preds[num] == 0 else 'green'
#                 #text = 'NoMask' if preds[num] == 0 else 'Mask'
#                 rect = mpatches.Rectangle((x1,y1), x2-x1, y2-y1, fill=False, ec=color, linewidth=2)
#                 axes.add_patch(rect)
#                 #axes.text(x1,y1, text, bbox={'facecolor': color, 'alpha': 0.5, 'pad': 3})

#             plt.savefig('/root/project_space/pred_images/maksssksksss502_preds_new.jpg')
        
#         self.logger.debug('Completed SavePredsMask')