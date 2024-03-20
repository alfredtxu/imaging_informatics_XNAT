 \o E:/logs/scanlist_full.csv
 SELECT public.xnat_imagescandata.ID, public.xnat_imagesessiondata.ID, public.xnat_imagesessiondata.uid FROM  public.xnat_imagescandata, public.xnat_imagesessiondata WHERE public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID;
 
 
  SELECT public.xnat_mrscandata.xnat_imagescandata_id, public.xnat_imagescandata.image_session_id, public.xnat_imagescandata.id, public.xnat_imagesessiondata.uid FROM public.xnat_mrscandata, public.xnat_imagescandata, public.xnat_imagesessiondata WHERE public.xnat_mrscandata.xnat_imagescandata_id=public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_mrscandata.parameters_voxelres_x is NULL AND public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID;
  
  
   SELECT  public.xnat_experimentdata.ID, public.xnat_experimentdata.label, public.xnat_subjectassessordata.subject_id FROM  public.xnat_experimentdata,    public.xnat_subjectassessordata WHERE public.xnat_subjectassessordata.id = public.xnat_experimentdata.ID;